package sshprofile

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"time"

	"golang.org/x/crypto/ssh"

	"strand/internal/tunnel"
	"strand/internal/web/store"
)

const MetaKey = "ssh_tunnel_profiles"

var idPattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,62}$`)

func ValidID(id string) bool { return idPattern.MatchString(id) }

type Profile struct {
	ID             string `json:"id"`
	Host           string `json:"host"`
	Port           int    `json:"port"`
	User           string `json:"user"`
	IdentityFile   string `json:"identity_file,omitempty"`
	SealedKey      []byte `json:"sealed_key,omitempty"`
	SealedPassword []byte `json:"sealed_password,omitempty"`
	HostKey        []byte `json:"host_key,omitempty"`
	// Account: name of the saved SSH account (user and credentials) this host signs in with. Empty: the
	// user and credentials are stored on the profile itself (profiles saved before accounts existed).
	Account string `json:"account,omitempty"`
}

func (p Profile) Address() string { return net.JoinHostPort(p.Host, strconv.Itoa(p.Port)) }

func (p Profile) SealKey(box *store.Box, key []byte) ([]byte, error) {
	if box == nil {
		return nil, errors.New("STRAND_WEB_SECRET_KEY is required for stored SSH keys")
	}
	return box.Seal(key, p.KeyAAD())
}

func (p Profile) OpenKey(box *store.Box) ([]byte, error) {
	if len(p.SealedKey) == 0 {
		return nil, nil
	}
	if box == nil {
		return nil, errors.New("STRAND_WEB_SECRET_KEY is required for stored SSH keys")
	}
	return box.Open(p.SealedKey, p.KeyAAD())
}

func (p Profile) KeyAAD() string { return "ssh-tunnel-profile:" + p.ID }

func (p Profile) PasswordAAD() string { return "ssh-tunnel-password:" + p.ID }

func (p Profile) SealPassword(box *store.Box, password []byte) ([]byte, error) {
	if box == nil {
		return nil, errors.New("STRAND_WEB_SECRET_KEY is required for stored SSH passwords")
	}
	return box.Seal(password, p.PasswordAAD())
}

type Metadata interface {
	MetaGet(context.Context, string) (string, bool, error)
	MetaSet(context.Context, string, string) error
}

func List(ctx context.Context, st Metadata) ([]Profile, error) {
	raw, ok, err := st.MetaGet(ctx, MetaKey)
	if err != nil || !ok {
		return nil, err
	}
	return decode(raw)
}

func decode(raw string) ([]Profile, error) {
	var profiles []Profile
	if err := json.Unmarshal([]byte(raw), &profiles); err != nil {
		return nil, errors.New("stored SSH tunnel profiles are invalid")
	}
	return profiles, nil
}

func Save(ctx context.Context, st Metadata, profiles []Profile) error {
	raw, err := json.Marshal(profiles)
	if err != nil {
		return err
	}
	return st.MetaSet(ctx, MetaKey, string(raw))
}

func Find(profiles []Profile, id string) (Profile, error) {
	for _, p := range profiles {
		if p.ID == id {
			return p, nil
		}
	}
	return Profile{}, fmt.Errorf("SSH tunnel profile %q was not found", id)
}

// LoadConfig reads the profile id and its account straight from the Web metadata database (a strand
// process started by the Web console) and returns its tunnel settings. box is called only when the
// profile or account holds sealed credentials.
func LoadConfig(ctx context.Context, dsn, id string, box func() (*store.Box, error)) (tunnel.Config, error) {
	if dsn == "" {
		return tunnel.Config{}, errors.New("SSH tunnel profiles require STRAND_WEB_DATABASE_DSN in the Strand process")
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return tunnel.Config{}, errors.New("invalid SSH profile database configuration")
	}
	defer db.Close()
	read := func(key string) (string, bool, error) {
		var raw string
		err := db.QueryRowContext(ctx, "SELECT value FROM web_meta WHERE key = $1", key).Scan(&raw)
		if errors.Is(err, sql.ErrNoRows) {
			return "", false, nil
		}
		return raw, err == nil, err
	}
	return resolveFrom(read, id, box)
}

// Config returns the tunnel settings of the saved profile id (resolving its account) from st.
func Config(ctx context.Context, st Metadata, id string, box *store.Box) (tunnel.Config, error) {
	read := func(key string) (string, bool, error) { return st.MetaGet(ctx, key) }
	return resolveFrom(read, id, func() (*store.Box, error) { return box, nil })
}

func resolveFrom(read func(string) (string, bool, error), id string, box func() (*store.Box, error)) (tunnel.Config, error) {
	raw, ok, err := read(MetaKey)
	if err != nil {
		return tunnel.Config{}, errors.New("could not read SSH tunnel profiles from the metadata database")
	}
	var profiles []Profile
	if ok {
		if profiles, err = decode(raw); err != nil {
			return tunnel.Config{}, err
		}
	}
	p, err := Find(profiles, id)
	if err != nil {
		return tunnel.Config{}, err
	}
	var accounts []Account
	if p.Account != "" {
		raw, ok, err := read(AccountsMetaKey)
		if err != nil {
			return tunnel.Config{}, errors.New("could not read SSH accounts from the metadata database")
		}
		if ok {
			if accounts, err = decodeAccounts(raw); err != nil {
				return tunnel.Config{}, err
			}
		}
	}
	var b *store.Box
	if p.sealed(accounts) {
		if b, err = box(); err != nil {
			return tunnel.Config{}, err
		}
	}
	return p.Resolve(accounts, b)
}

// sealed: the profile (or its account) holds encrypted credentials, so resolving it needs the Web secret key.
func (p Profile) sealed(accounts []Account) bool {
	if p.Account == "" {
		return len(p.SealedKey) > 0 || len(p.SealedPassword) > 0
	}
	a, err := FindAccount(accounts, p.Account)
	return err == nil && (len(a.SealedKey) > 0 || len(a.SealedPassword) > 0)
}

// Resolve returns the tunnel settings of p: its own credentials, or those of its account (found in accounts).
func (p Profile) Resolve(accounts []Account, box *store.Box) (tunnel.Config, error) {
	if p.Account == "" {
		return p.TunnelConfig(box)
	}
	a, err := FindAccount(accounts, p.Account)
	if err != nil {
		return tunnel.Config{}, fmt.Errorf("SSH tunnel %q: %w", p.ID, err)
	}
	if p.User != "" || p.IdentityFile != "" || len(p.SealedKey) > 0 || len(p.SealedPassword) > 0 {
		return tunnel.Config{}, fmt.Errorf("SSH tunnel %q has both an account and its own credentials", p.ID)
	}
	hostKey, err := p.hostKey()
	if err != nil {
		return tunnel.Config{}, err
	}
	cfg := tunnel.Config{SSHAddress: p.Address(), SSHUser: a.User, IdentityFile: a.IdentityFile, HostKey: hostKey}
	return withCredentials(cfg, a.SealedKey, a.SealedPassword, a.KeyAAD(), a.PasswordAAD(), box, "SSH account "+a.ID)
}

func (p Profile) hostKey() (ssh.PublicKey, error) {
	if len(p.HostKey) == 0 {
		return nil, nil
	}
	hostKey, err := ssh.ParsePublicKey(p.HostKey)
	if err != nil {
		return nil, errors.New("stored SSH host key is invalid")
	}
	return hostKey, nil
}

// TunnelConfig returns the settings of a profile that stores its own user and credentials (Account empty).
func (p Profile) TunnelConfig(box *store.Box) (tunnel.Config, error) {
	hostKey, err := p.hostKey()
	if err != nil {
		return tunnel.Config{}, err
	}
	cfg := tunnel.Config{SSHAddress: p.Address(), SSHUser: p.User, IdentityFile: p.IdentityFile, HostKey: hostKey}
	return withCredentials(cfg, p.SealedKey, p.SealedPassword, p.KeyAAD(), p.PasswordAAD(), box, "stored SSH profile")
}

// withCredentials opens the sealed key or password into cfg. Exactly one method may be set; neither means
// the identity file or the service's SSH agent.
func withCredentials(cfg tunnel.Config, sealedKey, sealedPassword []byte, keyAAD, passwordAAD string, box *store.Box, what string) (tunnel.Config, error) {
	if len(sealedPassword) != 0 {
		if len(sealedKey) != 0 || cfg.IdentityFile != "" {
			return tunnel.Config{}, errors.New(what + " has conflicting authentication methods")
		}
		if box == nil {
			return tunnel.Config{}, errors.New("STRAND_WEB_SECRET_KEY is required for stored SSH passwords")
		}
		password, err := box.Open(sealedPassword, passwordAAD)
		if err != nil {
			return tunnel.Config{}, errors.New("could not decrypt stored SSH password")
		}
		cfg.Password = password
		return cfg, nil
	}
	if len(sealedKey) == 0 {
		return cfg, nil
	}
	if box == nil {
		return tunnel.Config{}, errors.New("STRAND_WEB_SECRET_KEY is required for stored SSH keys")
	}
	key, err := box.Open(sealedKey, keyAAD)
	if err != nil {
		return tunnel.Config{}, errors.New("could not decrypt stored SSH private key")
	}
	cfg.PrivateKey = key
	return cfg, nil
}
