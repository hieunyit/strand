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

	"strand/internal/tunnel"
	"strand/internal/web/store"
)

const MetaKey = "ssh_tunnel_profiles"

var idPattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,62}$`)

func ValidID(id string) bool { return idPattern.MatchString(id) }

type Profile struct {
	ID           string `json:"id"`
	Host         string `json:"host"`
	Port         int    `json:"port"`
	User         string `json:"user"`
	IdentityFile string `json:"identity_file,omitempty"`
	SealedKey    []byte `json:"sealed_key,omitempty"`
}

func (p Profile) Address() string { return net.JoinHostPort(p.Host, strconv.Itoa(p.Port)) }

func (p Profile) SealKey(box *store.Box, key []byte) ([]byte, error) {
	if box == nil {
		return nil, errors.New("STRAND_WEB_SECRET_KEY is required for stored SSH keys")
	}
	return box.Seal(key, "ssh-tunnel-profile:"+p.ID)
}

func (p Profile) OpenKey(box *store.Box) ([]byte, error) {
	if len(p.SealedKey) == 0 {
		return nil, nil
	}
	if box == nil {
		return nil, errors.New("STRAND_WEB_SECRET_KEY is required for stored SSH keys")
	}
	return box.Open(p.SealedKey, "ssh-tunnel-profile:"+p.ID)
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

func Load(ctx context.Context, dsn, id string) (Profile, error) {
	if dsn == "" {
		return Profile{}, errors.New("SSH tunnel profiles require STRAND_WEB_DATABASE_DSN in the Strand process")
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return Profile{}, errors.New("invalid SSH profile database configuration")
	}
	defer db.Close()
	var raw string
	err = db.QueryRowContext(ctx, "SELECT value FROM web_meta WHERE key = $1", MetaKey).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return Find(nil, id)
	}
	if err != nil {
		return Profile{}, errors.New("could not read SSH tunnel profiles from the metadata database")
	}
	profiles, err := decode(raw)
	if err != nil {
		return Profile{}, err
	}
	return Find(profiles, id)
}

func (p Profile) TunnelConfig(box *store.Box) (tunnel.Config, error) {
	key, err := p.OpenKey(box)
	if err != nil {
		return tunnel.Config{}, err
	}
	return tunnel.Config{SSHAddress: p.Address(), SSHUser: p.User, IdentityFile: p.IdentityFile, PrivateKey: key}, nil
}
