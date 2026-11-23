package web

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"golang.org/x/crypto/ssh"

	"strand/internal/config"
	"strand/internal/sshprofile"
	"strand/internal/tunnel"
)

type sshProfileView struct {
	ID, Host, User string
	Port           int
	HasStoredKey   bool
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
		views = append(views, sshProfileView{ID: p.ID, Host: p.Host, Port: p.Port, User: p.User, HasStoredKey: len(p.SealedKey) > 0})
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

func (s *Server) saveSSHProfile(r *http.Request) error {
	if s.db == nil {
		return errors.New("SSH tunnel profiles require the metadata database")
	}
	port := 22
	if raw := strings.TrimSpace(r.FormValue("profile_port")); raw != "" {
		var err error
		port, err = strconv.Atoi(raw)
		if err != nil || port < 1 || port > 65535 {
			return errors.New("SSH port must be between 1 and 65535")
		}
	}
	p := sshprofile.Profile{ID: strings.TrimSpace(r.FormValue("profile_id")), Host: strings.TrimSpace(r.FormValue("profile_host")),
		User: strings.TrimSpace(r.FormValue("profile_user")), Port: port, IdentityFile: strings.TrimSpace(r.FormValue("profile_identity_file"))}
	if !sshprofile.ValidID(p.ID) {
		return errors.New("profile name must begin with an alphanumeric character and use at most 63 letters, digits, dots, underscores or hyphens")
	}
	c := config.SSHTunnelConfig{Host: p.Host, Port: p.Port, User: p.User, IdentityFile: p.IdentityFile}
	if err := c.Validate("mysql"); err != nil {
		return err
	}
	if p.IdentityFile != "" {
		knownHosts, err := tunnel.ConfiguredKnownHosts()
		if err != nil {
			return err
		}
		if err := tunnel.ValidateSecurity(p.IdentityFile, knownHosts); err != nil {
			return err
		}
	}
	if raw := strings.TrimSpace(r.FormValue("profile_private_key")); raw != "" {
		if len(raw) > 48<<10 || p.IdentityFile != "" {
			return errors.New("choose a private key or an identity file; private key limit is 48 KiB")
		}
		key := []byte(raw + "\n")
		defer clear(key)
		if _, err := ssh.ParsePrivateKey(key); err != nil {
			return errors.New("invalid or passphrase-protected SSH private key; use an SSH agent for passphrase-protected keys")
		}
		var err error
		p.SealedKey, err = p.SealKey(s.db.box, key)
		if err != nil {
			return errors.New("could not encrypt the SSH private key")
		}
	}
	return s.db.st.Locked(r.Context(), sshprofile.MetaKey, func(ctx context.Context) error {
		profiles, err := sshprofile.List(ctx, s.db.st)
		if err != nil {
			return errors.New("could not read SSH tunnel profiles")
		}
		if _, err := sshprofile.Find(profiles, p.ID); err == nil {
			return fmt.Errorf("SSH tunnel profile %q already exists; choose a new name", p.ID)
		}
		if len(profiles) >= 128 {
			return errors.New("SSH tunnel profile limit reached")
		}
		detail := map[string]any{"profile": p.ID, "host": p.Host, "user": p.User, "stored_key": len(p.SealedKey) > 0}
		if err := s.auditSensitive(r, "tunnel.profile.create", detail, "started"); err != nil {
			return err
		}
		if err := sshprofile.Save(ctx, s.db.st, append(profiles, p)); err != nil {
			_ = s.auditSensitive(r, "tunnel.profile.create", detail, "error")
			return errors.New("could not save SSH tunnel profile")
		}
		if err := s.auditSensitive(r, "tunnel.profile.create", detail, "ok"); err != nil {
			s.log.Warn("could not record SSH tunnel profile creation outcome", "profile", p.ID)
		}
		return nil
	})
}
