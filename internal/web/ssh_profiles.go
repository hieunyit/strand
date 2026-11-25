package web

import (
	"context"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"net/http"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	"golang.org/x/crypto/ssh"

	"strand/internal/config"
	"strand/internal/sshprofile"
	"strand/internal/tunnel"
)

type sshProfileView struct {
	ID, Host, User, Address string
	Port                    int
}

type pipelineSSHFieldsView struct {
	Side     string
	Profiles []sshProfileView
}

func (s *Server) sshProfileViews(ctx context.Context) []sshProfileView {
	if s.db == nil {
		return nil
	}
	profiles, err := sshprofile.List(ctx, s.db.st)
	if err != nil {
		s.log.Warn("cannot list SSH tunnel profiles", "error", err)
		return nil
	}
	views := make([]sshProfileView, 0, len(profiles))
	for _, p := range profiles {
		views = append(views, sshProfileView{ID: p.ID, Host: p.Host, Port: p.Port, User: p.User, Address: p.Address()})
	}
	return views
}

func (s *Server) savedTunnel(ctx context.Context, id string) (tunnel.Config, error) {
	if s.db == nil || !sshprofile.ValidID(id) {
		return tunnel.Config{}, errors.New("invalid or unavailable SSH tunnel profile")
	}
	profiles, err := sshprofile.List(ctx, s.db.st)
	if err != nil {
		return tunnel.Config{}, errors.New("could not read SSH tunnel profiles")
	}
	p, err := sshprofile.Find(profiles, id)
	if err != nil {
		return tunnel.Config{}, err
	}
	return p.TunnelConfig(s.db.box)
}

type sshProfileForm struct {
	ID, Host, Port, User string
	Edit                 bool
}

func sshProfileFormFromRequest(r *http.Request) sshProfileForm {
	return sshProfileForm{
		ID: strings.TrimSpace(r.PostForm.Get("profile_id")), Host: strings.TrimSpace(r.PostForm.Get("profile_host")),
		Port: strings.TrimSpace(r.PostForm.Get("profile_port")), User: strings.TrimSpace(r.PostForm.Get("profile_user")),
		Edit: r.PostForm.Get("profile_edit") == "1",
	}
}

func (f sshProfileForm) profile() (sshprofile.Profile, error) {
	p := sshprofile.Profile{ID: f.ID, Host: f.Host, User: f.User, Port: 22}
	if !sshprofile.ValidID(p.ID) {
		return p, errors.New("tunnel name must begin with a letter or digit and use at most 63 letters, digits, dots, underscores or hyphens")
	}
	port := f.Port
	if host, embeddedPort, err := net.SplitHostPort(f.Host); err == nil {
		if port != "" && port != embeddedPort {
			return p, errors.New("SSH host and port fields specify different ports")
		}
		p.Host, port = host, embeddedPort
	} else if strings.HasPrefix(f.Host, "[") && strings.HasSuffix(f.Host, "]") {
		p.Host = f.Host[1 : len(f.Host)-1]
	}
	if port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return p, errors.New("SSH port must be between 1 and 65535")
		}
		p.Port = n
	}
	return p, (&config.SSHTunnelConfig{Host: p.Host, Port: p.Port, User: p.User}).Validate("mysql")
}

type sshHostPreview struct {
	Address, Fingerprint, KeyType string
}

func sshProfileHostPreview(r *http.Request, form sshProfileForm, show bool) (*sshHostPreview, ssh.PublicKey, error) {
	p, err := form.profile()
	if err != nil {
		return nil, nil, err
	}
	if !show && r.PostForm.Get("profile_host_address") != p.Address() {
		return nil, nil, errors.New("SSH address changed. Show its fingerprint again")
	}
	ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
	defer cancel()
	key, err := tunnel.ProbeHostKey(ctx, p.Address())
	if err != nil {
		return nil, nil, fmt.Errorf("could not read the SSH host key for %s: %w", p.Address(), err)
	}
	preview := &sshHostPreview{Address: p.Address(), Fingerprint: ssh.FingerprintSHA256(key), KeyType: key.Type()}
	if show {
		return preview, nil, nil
	}
	if r.PostForm.Get("profile_fingerprint") != preview.Fingerprint {
		return preview, nil, errors.New("the SSH host key changed. Verify the new fingerprint before confirming")
	}
	if r.PostForm.Get("profile_confirm_host_key") != "1" {
		return preview, nil, errors.New("confirm the SSH host fingerprint before saving")
	}
	return preview, key, nil
}

func (s *Server) saveSSHProfile(r *http.Request, form sshProfileForm, hostKey ssh.PublicKey) error {
	p, err := form.profile()
	if err != nil {
		return err
	}
	profiles, err := sshprofile.List(r.Context(), s.db.st)
	if err != nil {
		return errors.New("could not read SSH tunnels from the metadata database")
	}
	index := slices.IndexFunc(profiles, func(v sshprofile.Profile) bool { return v.ID == p.ID })
	if form.Edit != (index >= 0) {
		if form.Edit {
			return errors.New("SSH tunnel no longer exists")
		}
		return fmt.Errorf("SSH tunnel %q already exists. Choose Edit in the saved tunnels list", p.ID)
	}
	if index < 0 && len(profiles) >= 128 {
		return errors.New("SSH tunnel limit reached")
	}
	var previous sshprofile.Profile
	action := "tunnel.profile.create"
	if form.Edit {
		previous = profiles[index]
		p.IdentityFile, p.SealedKey = previous.IdentityFile, previous.SealedKey
		action = "tunnel.profile.update"
		if p.Address() == previous.Address() {
			p.HostKey = previous.HostKey
		} else if len(previous.HostKey) != 0 && hostKey == nil {
			return errors.New("SSH address changed. Show and confirm the new fingerprint before saving")
		}
	}
	if hostKey != nil {
		p.HostKey = hostKey.Marshal()
	}
	key, err := profilePrivateKey(r.PostForm.Get("profile_private_key"), r.PostForm.Get("profile_key_passphrase"))
	if err != nil {
		return err
	}
	if len(key) != 0 {
		defer clear(key)
		p.SealedKey, err = p.SealKey(s.db.box, key)
		if err != nil {
			return err
		}
		p.IdentityFile = ""
	}
	if err := s.checkSSHProfile(r.Context(), p); err != nil {
		return err
	}
	return s.db.st.Locked(r.Context(), sshprofile.MetaKey, func(ctx context.Context) error {
		profiles, err := sshprofile.List(ctx, s.db.st)
		if err != nil {
			return errors.New("could not read SSH tunnels from the metadata database")
		}
		index := slices.IndexFunc(profiles, func(v sshprofile.Profile) bool { return v.ID == p.ID })
		if form.Edit {
			if index < 0 || !reflect.DeepEqual(previous, profiles[index]) {
				return errors.New("SSH tunnel changed while it was being checked. Reload it and try again")
			}
			profiles[index] = p
		} else {
			if index >= 0 || len(profiles) >= 128 {
				return errors.New("SSH tunnel name or limit changed while it was being checked")
			}
			profiles = append(profiles, p)
		}
		detail := map[string]any{"profile": p.ID, "host": p.Host, "user": p.User, "stored_key": len(p.SealedKey) > 0}
		if hostKey != nil {
			detail["host_key_fingerprint"] = ssh.FingerprintSHA256(hostKey)
		}
		if err := s.auditSensitive(r, action, detail, "started"); err != nil {
			return err
		}
		if err := sshprofile.Save(ctx, s.db.st, profiles); err != nil {
			_ = s.auditSensitive(r, action, detail, "error")
			return errors.New("could not save SSH tunnel in the metadata database")
		}
		if err := s.auditSensitive(r, action, detail, "ok"); err != nil {
			s.log.Warn("could not record SSH tunnel save outcome", "profile", p.ID)
		}
		return nil
	})
}

// profilePrivateKey unlocks the submitted key for sealing with the Web secret key.
// The original passphrase is only used during this request.
func profilePrivateKey(raw, passphrase string) ([]byte, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		if passphrase != "" {
			return nil, errors.New("paste the SSH private key that uses this passphrase")
		}
		return nil, nil
	}
	if len(raw) > 48<<10 {
		return nil, errors.New("SSH private key exceeds 48 KiB")
	}
	if len(passphrase) > 4<<10 {
		return nil, errors.New("SSH key passphrase exceeds 4 KiB")
	}
	key := []byte(raw + "\n")
	if _, err := ssh.ParsePrivateKey(key); err == nil {
		if passphrase != "" {
			clear(key)
			return nil, errors.New("leave the passphrase blank for an unencrypted SSH private key")
		}
		return key, nil
	} else {
		var protected *ssh.PassphraseMissingError
		if !errors.As(err, &protected) {
			clear(key)
			return nil, errors.New("invalid or unsupported SSH private key format")
		}
	}
	defer clear(key)
	if passphrase == "" {
		return nil, errors.New("enter the passphrase for this SSH private key")
	}
	password := []byte(passphrase)
	defer clear(password)
	privateKey, err := ssh.ParseRawPrivateKeyWithPassphrase(key, password)
	if err != nil {
		return nil, errors.New("could not unlock the SSH private key. Check the passphrase and key format")
	}
	block, err := ssh.MarshalPrivateKey(privateKey, "")
	if err != nil {
		return nil, errors.New("could not encode the unlocked SSH private key")
	}
	defer clear(block.Bytes)
	return pem.EncodeToMemory(block), nil
}

func (s *Server) checkSSHProfile(ctx context.Context, p sshprofile.Profile) error {
	cfg, err := p.TunnelConfig(s.db.box)
	if err != nil {
		return err
	}
	defer clear(cfg.PrivateKey)
	if err := cfg.PrepareServiceTrust(); err != nil {
		if cfg.HostKey == nil && !strings.HasPrefix(err.Error(), "identity_file:") {
			return fmt.Errorf("use Show fingerprint to verify this SSH server on the Web: %w", err)
		}
		return fmt.Errorf("SSH key configuration is invalid on the server: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	if err := tunnel.Check(ctx, cfg); err != nil {
		return fmt.Errorf("SSH connection check failed for %s: %w", p.Address(), err)
	}
	return nil
}
