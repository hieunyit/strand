package backup

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
)

// Encryption key environment variables. Keys are 32 random bytes, base64 (standard or URL) or 64 hex
// characters. The FIRST key listed is the active key for new backups; every listed key stays usable for
// reading, so rotation = put the new key first and keep the old ones until their backups expire.
const (
	EnvEncryptionKeys     = "STRAND_BACKUP_ENCRYPTION_KEYS"      // "id1:key1,id2:key2"
	EnvEncryptionKeysFile = "STRAND_BACKUP_ENCRYPTION_KEYS_FILE" // file of "id:key" lines (# comments)
	EnvEncryptionKey      = "STRAND_BACKUP_ENCRYPTION_KEY"       // single key, id from ..._KEY_ID or "default"
	EnvEncryptionKeyFile  = "STRAND_BACKUP_ENCRYPTION_KEY_FILE"  // file containing a single key
	EnvEncryptionKeyID    = "STRAND_BACKUP_ENCRYPTION_KEY_ID"
)

var keyIDRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

// Key is one named AES-256 master key.
type Key struct {
	ID     string
	Secret []byte
}

func (k *Key) validate() error {
	if k == nil || !keyIDRE.MatchString(k.ID) {
		return errors.New("backup encryption: key id must be 1-64 letters, digits, '.', '_' or '-'")
	}
	if len(k.Secret) != 32 {
		return errors.New("backup encryption: key must be exactly 32 bytes")
	}
	return nil
}

// Fingerprint is a short non-reversible identifier of the key material (for logs and the UI).
func (k Key) Fingerprint() string {
	h := sha256.Sum256(append([]byte("strand-backup-key-fingerprint:"), k.Secret...))
	return hex.EncodeToString(h[:6])
}

// Keyring holds every configured key; Active encrypts new backups.
type Keyring struct {
	Active *Key
	keys   map[string]Key
}

// Get returns the key with id.
func (r *Keyring) Get(id string) (Key, bool) {
	if r == nil {
		return Key{}, false
	}
	k, ok := r.keys[id]
	return k, ok
}

// IDs lists configured key ids (sorted).
func (r *Keyring) IDs() []string {
	if r == nil {
		return nil
	}
	out := make([]string, 0, len(r.keys))
	for id := range r.keys {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// NewKeyring builds a keyring; the first key is active.
func NewKeyring(keys ...Key) (*Keyring, error) {
	r := &Keyring{keys: map[string]Key{}}
	for _, k := range keys {
		if err := r.add(k); err != nil {
			return nil, err
		}
	}
	return r, nil
}

func (r *Keyring) add(k Key) error {
	if err := k.validate(); err != nil {
		return err
	}
	if old, ok := r.keys[k.ID]; ok {
		if !bytes.Equal(old.Secret, k.Secret) {
			return fmt.Errorf("backup encryption: key id %q is configured twice with different key material", k.ID)
		}
		return nil
	}
	r.keys[k.ID] = k
	if r.Active == nil {
		kk := k
		r.Active = &kk
	}
	return nil
}

// LoadKeyring reads keys from the environment (see EnvEncryption*). It returns (nil, nil) when no key is
// configured. Error messages never contain key material.
func LoadKeyring(getenv func(string) string) (*Keyring, error) {
	r := &Keyring{keys: map[string]Key{}}
	if v := strings.TrimSpace(getenv(EnvEncryptionKeys)); v != "" {
		for i, item := range strings.Split(v, ",") {
			k, err := parseKeyEntry(strings.TrimSpace(item))
			if err != nil {
				return nil, fmt.Errorf("%s entry %d: %w", EnvEncryptionKeys, i+1, err)
			}
			if err := r.add(k); err != nil {
				return nil, err
			}
		}
	}
	if path := strings.TrimSpace(getenv(EnvEncryptionKeysFile)); path != "" {
		data, err := readSecretFile(path)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", EnvEncryptionKeysFile, err)
		}
		sc := bufio.NewScanner(bytes.NewReader(data))
		line := 0
		for sc.Scan() {
			line++
			text := strings.TrimSpace(sc.Text())
			if text == "" || strings.HasPrefix(text, "#") {
				continue
			}
			k, err := parseKeyEntry(text)
			if err != nil {
				return nil, fmt.Errorf("%s line %d: %w", EnvEncryptionKeysFile, line, err)
			}
			if err := r.add(k); err != nil {
				return nil, err
			}
		}
	}
	single := strings.TrimSpace(getenv(EnvEncryptionKey))
	if path := strings.TrimSpace(getenv(EnvEncryptionKeyFile)); path != "" {
		if single != "" {
			return nil, fmt.Errorf("set only one of %s and %s", EnvEncryptionKey, EnvEncryptionKeyFile)
		}
		data, err := readSecretFile(path)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", EnvEncryptionKeyFile, err)
		}
		single = strings.TrimSpace(string(data))
	}
	if single != "" {
		id := strings.TrimSpace(getenv(EnvEncryptionKeyID))
		if id == "" {
			id = "default"
		}
		secret, err := decodeKey(single)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", EnvEncryptionKey, err)
		}
		if err := r.add(Key{ID: id, Secret: secret}); err != nil {
			return nil, err
		}
	}
	if len(r.keys) == 0 {
		return nil, nil
	}
	return r, nil
}

func parseKeyEntry(s string) (Key, error) {
	id, raw, ok := strings.Cut(s, ":")
	if !ok {
		id, raw, ok = strings.Cut(s, "=")
	}
	if !ok {
		return Key{}, errors.New("want id:key")
	}
	secret, err := decodeKey(strings.TrimSpace(raw))
	if err != nil {
		return Key{}, err
	}
	k := Key{ID: strings.TrimSpace(id), Secret: secret}
	return k, k.validate()
}

func decodeKey(s string) ([]byte, error) {
	if len(s) == 64 {
		if b, err := hex.DecodeString(s); err == nil {
			return b, nil
		}
	}
	for _, enc := range []*base64.Encoding{base64.StdEncoding, base64.URLEncoding, base64.RawStdEncoding, base64.RawURLEncoding} {
		if b, err := enc.DecodeString(s); err == nil {
			if len(b) != 32 {
				return nil, errors.New("key must decode to exactly 32 bytes (generate with: openssl rand -base64 32)")
			}
			return b, nil
		}
	}
	return nil, errors.New("key is neither base64 nor 64 hex characters")
}

// readSecretFile refuses key files readable by group or others.
func readSecretFile(path string) ([]byte, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if fi.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("%s must not be accessible by group or others (chmod 600)", path)
	}
	return os.ReadFile(path)
}

// GenerateKeyBase64 returns a new random key in base64 (for `strand backup keygen`).
func GenerateKeyBase64(randRead func([]byte) (int, error)) (string, error) {
	b := make([]byte, 32)
	if _, err := randRead(b); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(b), nil
}
