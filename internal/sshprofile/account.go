package sshprofile

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"strand/internal/web/store"
)

// AccountsMetaKey: web_meta key of the saved SSH accounts (JSON list of Account).
const AccountsMetaKey = "ssh_tunnel_accounts"

// MaxAccounts bounds the saved SSH accounts.
const MaxAccounts = 128

// Account is a reusable SSH sign-in: a user and one credential (private key or password, sealed with the
// Web secret key), or neither (the service's SSH agent / identity file). Several host profiles can use it.
type Account struct {
	ID             string `json:"id"`
	User           string `json:"user"`
	IdentityFile   string `json:"identity_file,omitempty"`
	SealedKey      []byte `json:"sealed_key,omitempty"`
	SealedPassword []byte `json:"sealed_password,omitempty"`
}

// KeyAAD / PasswordAAD bind sealed credentials to the account name: a ciphertext copied to another
// account (or to a profile) does not open.
func (a Account) KeyAAD() string      { return "ssh-account-key:" + a.ID }
func (a Account) PasswordAAD() string { return "ssh-account-password:" + a.ID }

// AuthMode: "password", "key" (stored private key) or "agent" (no stored credential).
func (a Account) AuthMode() string {
	switch {
	case len(a.SealedPassword) > 0:
		return "password"
	case len(a.SealedKey) > 0:
		return "key"
	}
	return "agent"
}

func (a Account) SealKey(box *store.Box, key []byte) ([]byte, error) {
	if box == nil {
		return nil, errors.New("STRAND_WEB_SECRET_KEY is required for stored SSH keys")
	}
	return box.Seal(key, a.KeyAAD())
}

func (a Account) SealPassword(box *store.Box, password []byte) ([]byte, error) {
	if box == nil {
		return nil, errors.New("STRAND_WEB_SECRET_KEY is required for stored SSH passwords")
	}
	return box.Seal(password, a.PasswordAAD())
}

func ListAccounts(ctx context.Context, st Metadata) ([]Account, error) {
	raw, ok, err := st.MetaGet(ctx, AccountsMetaKey)
	if err != nil || !ok {
		return nil, err
	}
	return decodeAccounts(raw)
}

func decodeAccounts(raw string) ([]Account, error) {
	var accounts []Account
	if err := json.Unmarshal([]byte(raw), &accounts); err != nil {
		return nil, errors.New("stored SSH accounts are invalid")
	}
	return accounts, nil
}

func SaveAccounts(ctx context.Context, st Metadata, accounts []Account) error {
	raw, err := json.Marshal(accounts)
	if err != nil {
		return err
	}
	return st.MetaSet(ctx, AccountsMetaKey, string(raw))
}

func FindAccount(accounts []Account, id string) (Account, error) {
	for _, a := range accounts {
		if a.ID == id {
			return a, nil
		}
	}
	return Account{}, fmt.Errorf("SSH account %q was not found", id)
}

// UsedBy lists the profiles that sign in with account id.
func UsedBy(profiles []Profile, id string) []string {
	var out []string
	for _, p := range profiles {
		if p.Account == id {
			out = append(out, p.ID)
		}
	}
	return out
}
