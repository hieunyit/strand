package web

import (
	"context"
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

func (s *Server) saveSSHProfile(r *http.Request, form sshProfileForm) error {
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
	}
	if raw := strings.TrimSpace(r.PostForm.Get("profile_private_key")); raw != "" {
		if len(raw) > 48<<10 {
			return errors.New("SSH private key exceeds 48 KiB")
		}
		key := []byte(raw + "\n")
		defer clear(key)
		if _, err := ssh.ParsePrivateKey(key); err != nil {
			return errors.New("invalid or passphrase-protected SSH private key. Use an SSH agent for passphrase-protected keys")
		}
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

func (s *Server) checkSSHProfile(ctx context.Context, p sshprofile.Profile) error {
	cfg, err := p.TunnelConfig(s.db.box)
	if err != nil {
		return err
	}
	defer clear(cfg.PrivateKey)
	cfg.HostKeysFile, err = tunnel.ConfiguredKnownHosts()
	if err != nil {
		return fmt.Errorf("SSH trust is unavailable. Check the server's trusted host keys: %w", err)
	}
	if err := tunnel.ValidateSecurity(cfg.IdentityFile, cfg.HostKeysFile); err != nil {
		if strings.HasPrefix(err.Error(), "known_hosts:") {
			return fmt.Errorf("configure the server's trusted SSH host keys with STRAND_WEB_SSH_KNOWN_HOSTS: %w", err)
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
