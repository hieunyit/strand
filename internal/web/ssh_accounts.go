package web

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"slices"
	"strings"

	"strand/internal/sshprofile"
)

// SSH accounts: a user and one credential (private key or password, sealed with the Web secret key) that
// several saved SSH hosts share. A host chooses an account instead of repeating the user and credential;
// changing the account's key or password changes it for every host that uses it. Accounts live in web_meta
// (sshprofile.AccountsMetaKey) next to the hosts (sshprofile.MetaKey).

type sshAccountView struct {
	ID, User, AuthMode string
	UsedBy             []string
}

type sshAccountForm struct {
	ID, User, AuthMode string
	Edit               bool
}

func sshAccountFormFromRequest(r *http.Request) sshAccountForm {
	return sshAccountForm{
		ID: strings.TrimSpace(r.PostForm.Get("account_id")), User: strings.TrimSpace(r.PostForm.Get("account_user")),
		AuthMode: r.PostForm.Get("account_auth_mode"), Edit: r.PostForm.Get("account_edit") == "1",
	}
}

func (s *Server) sshAccountViews(ctx context.Context) []sshAccountView {
	if s.db == nil {
		return nil
	}
	accounts, err := sshprofile.ListAccounts(ctx, s.db.st)
	if err != nil {
		s.log.Warn("cannot list SSH accounts", "error", err)
		return nil
	}
	profiles, _ := sshprofile.List(ctx, s.db.st)
	views := make([]sshAccountView, 0, len(accounts))
	for _, a := range accounts {
		views = append(views, sshAccountView{ID: a.ID, User: a.User, AuthMode: a.AuthMode(), UsedBy: sshprofile.UsedBy(profiles, a.ID)})
	}
	return views
}

// sshAccountFromForm validates the account fields and seals its credential. previous: the stored account
// being edited (nil for a new one); existing: every saved account (name clash, limit).
func (s *Server) sshAccountFromForm(r *http.Request, f sshAccountForm, previous *sshprofile.Account, existing []sshprofile.Account) (sshprofile.Account, error) {
	a := sshprofile.Account{ID: f.ID, User: f.User}
	if !sshprofile.ValidID(a.ID) {
		return a, errors.New("account name must begin with a letter or digit and use at most 63 letters, digits, dots, underscores or hyphens")
	}
	if a.User == "" || len(a.User) > 255 || strings.ContainsAny(a.User, " \t\r\n\x00@") {
		return a, errors.New("SSH user is required and must not contain spaces or @")
	}
	if previous == nil {
		if _, err := sshprofile.FindAccount(existing, a.ID); err == nil {
			return a, fmt.Errorf("SSH account %q already exists. Choose it in the list, or use another name", a.ID)
		}
		if len(existing) >= sshprofile.MaxAccounts {
			return a, errors.New("SSH account limit reached")
		}
	}
	var prev sealedCreds
	keep := false
	if previous != nil {
		prev = sealedCreds{Key: previous.SealedKey, Password: previous.SealedPassword, IdentityFile: previous.IdentityFile}
		keep = previous.User == a.User
	}
	got, err := sealSSHCredentials(r, "account_", f.AuthMode, prev, keep,
		func(b []byte) ([]byte, error) { return a.SealKey(s.db.box, b) },
		func(b []byte) ([]byte, error) { return a.SealPassword(s.db.box, b) })
	if err != nil {
		return a, err
	}
	a.SealedKey, a.SealedPassword, a.IdentityFile = got.Key, got.Password, got.IdentityFile
	return a, nil
}

// storeSSHAccount writes a (new when previous is nil, else replacing previous) under the accounts lock,
// with an audit record (no secrets).
func (s *Server) storeSSHAccount(r *http.Request, a sshprofile.Account, previous *sshprofile.Account) error {
	action := "tunnel.account.create"
	if previous != nil {
		action = "tunnel.account.update"
	}
	return s.db.st.Locked(r.Context(), sshprofile.AccountsMetaKey, func(ctx context.Context) error {
		accounts, err := sshprofile.ListAccounts(ctx, s.db.st)
		if err != nil {
			return errors.New("could not read SSH accounts from the metadata database")
		}
		index := slices.IndexFunc(accounts, func(v sshprofile.Account) bool { return v.ID == a.ID })
		if previous != nil {
			if index < 0 || !reflect.DeepEqual(*previous, accounts[index]) {
				return errors.New("SSH account changed while it was being checked. Reload it and try again")
			}
			accounts[index] = a
		} else {
			if index >= 0 || len(accounts) >= sshprofile.MaxAccounts {
				return errors.New("SSH account name or limit changed while it was being checked")
			}
			accounts = append(accounts, a)
		}
		detail := map[string]any{"account": a.ID, "user": a.User, "auth": a.AuthMode()}
		if err := s.auditSensitive(r, action, detail, "started"); err != nil {
			return err
		}
		if err := sshprofile.SaveAccounts(ctx, s.db.st, accounts); err != nil {
			_ = s.auditSensitive(r, action, detail, "error")
			return errors.New("could not save SSH account in the metadata database")
		}
		if err := s.auditSensitive(r, action, detail, "ok"); err != nil {
			s.log.Warn("could not record SSH account save outcome", "account", a.ID)
		}
		return nil
	})
}

// saveSSHAccountFromPage handles the account form of the SSH page. An edited account that hosts use is
// checked against each of them (SSH login) before it is stored: a wrong key or password is refused instead
// of breaking those hosts. A new or unused account cannot be checked yet; it is checked when a host uses it.
func (s *Server) saveSSHAccountFromPage(r *http.Request, f sshAccountForm) (checked []string, err error) {
	accounts, err := sshprofile.ListAccounts(r.Context(), s.db.st)
	if err != nil {
		return nil, errors.New("could not read SSH accounts from the metadata database")
	}
	var previous *sshprofile.Account
	if f.Edit {
		a, err := sshprofile.FindAccount(accounts, f.ID)
		if err != nil {
			return nil, errors.New("SSH account no longer exists")
		}
		previous = &a
	}
	a, err := s.sshAccountFromForm(r, f, previous, accounts)
	if err != nil {
		return nil, err
	}
	if previous != nil {
		profiles, err := sshprofile.List(r.Context(), s.db.st)
		if err != nil {
			return nil, errors.New("could not read SSH tunnels from the metadata database")
		}
		next := slices.Clone(accounts)
		next[slices.IndexFunc(next, func(v sshprofile.Account) bool { return v.ID == a.ID })] = a
		for _, p := range profiles {
			if p.Account != a.ID {
				continue
			}
			if err := s.checkSSHProfile(r.Context(), p, next); err != nil {
				return nil, fmt.Errorf("not saved: host %s does not accept this account: %w", p.ID, err)
			}
			checked = append(checked, p.ID)
		}
	}
	return checked, s.storeSSHAccount(r, a, previous)
}

// deleteSSHAccount removes an account no host uses.
func (s *Server) deleteSSHAccount(r *http.Request, id string) error {
	if !sshprofile.ValidID(id) {
		return errors.New("invalid SSH account name")
	}
	unlockedProfiles, err := sshprofile.List(r.Context(), s.db.st)
	if err != nil {
		return errors.New("could not read SSH tunnels from the metadata database")
	}
	if used := sshprofile.UsedBy(unlockedProfiles, id); len(used) > 0 {
		return fmt.Errorf("SSH account %q is used by %s. Choose another account on those hosts first", id, strings.Join(used, ", "))
	}
	// Hold the hosts lock so no host starts using the account while it is removed.
	return s.db.st.Locked(r.Context(), sshprofile.MetaKey, func(ctx context.Context) error {
		profiles, err := sshprofile.List(ctx, s.db.st)
		if err != nil {
			return errors.New("could not read SSH tunnels from the metadata database")
		}
		if used := sshprofile.UsedBy(profiles, id); len(used) > 0 {
			return fmt.Errorf("SSH account %q is used by %s", id, strings.Join(used, ", "))
		}
		return s.db.st.Locked(ctx, sshprofile.AccountsMetaKey, func(ctx context.Context) error {
			accounts, err := sshprofile.ListAccounts(ctx, s.db.st)
			if err != nil {
				return errors.New("could not read SSH accounts from the metadata database")
			}
			index := slices.IndexFunc(accounts, func(v sshprofile.Account) bool { return v.ID == id })
			if index < 0 {
				return errors.New("SSH account no longer exists")
			}
			detail := map[string]any{"account": id, "user": accounts[index].User}
			if err := s.auditSensitive(r, "tunnel.account.delete", detail, "started"); err != nil {
				return err
			}
			if err := sshprofile.SaveAccounts(ctx, s.db.st, slices.Delete(accounts, index, index+1)); err != nil {
				_ = s.auditSensitive(r, "tunnel.account.delete", detail, "error")
				return errors.New("could not delete SSH account in the metadata database")
			}
			_ = s.auditSensitive(r, "tunnel.account.delete", detail, "ok")
			return nil
		})
	})
}
