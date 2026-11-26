package sshprofile

import (
	"context"
	"strings"
	"testing"

	"strand/internal/web/store"
)

func TestResolveThroughAccount(t *testing.T) {
	box, err := store.NewBox(strings.Repeat("ab", 32))
	if err != nil {
		t.Fatal(err)
	}
	ops := Account{ID: "ops", User: "strand"}
	if ops.SealedPassword, err = ops.SealPassword(box, []byte("pw-fixture")); err != nil {
		t.Fatal(err)
	}
	st := store.NewMem()
	ctx := context.Background()
	if err := SaveAccounts(ctx, st, []Account{ops}); err != nil {
		t.Fatal(err)
	}
	if err := Save(ctx, st, []Profile{{ID: "a", Host: "bastion", Port: 22, Account: "ops"}, {ID: "gone", Host: "b", Port: 22, Account: "missing"}}); err != nil {
		t.Fatal(err)
	}
	cfg, err := Config(ctx, st, "a", box)
	if err != nil || cfg.SSHUser != "strand" || string(cfg.Password) != "pw-fixture" || cfg.SSHAddress != "bastion:22" {
		t.Fatalf("resolve via account: %+v %v", cfg, err)
	}
	if _, err := Config(ctx, st, "gone", box); err == nil || !strings.Contains(err.Error(), `SSH account "missing" was not found`) {
		t.Fatalf("missing account: %v", err)
	}
	// The ciphertext is bound to the account name: copied to another account it does not open.
	other := Account{ID: "other", User: "strand", SealedPassword: ops.SealedPassword}
	if _, err := (Profile{ID: "x", Host: "h", Port: 22, Account: "other"}).Resolve([]Account{other}, box); err == nil {
		t.Fatal("sealed password opened under another account name")
	}
	// A host may not mix an account with its own credentials.
	if _, err := (Profile{ID: "x", Host: "h", Port: 22, Account: "ops", User: "root"}).Resolve([]Account{ops}, box); err == nil {
		t.Fatal("profile with an account and its own user accepted")
	}
	// No secret key needed for an account without stored credentials (SSH agent).
	agent := Account{ID: "agent", User: "deploy"}
	if cfg, err := (Profile{ID: "y", Host: "h", Port: 2222, Account: "agent"}).Resolve([]Account{agent}, nil); err != nil || cfg.SSHUser != "deploy" || cfg.Password != nil || cfg.PrivateKey != nil {
		t.Fatalf("agent account: %+v %v", cfg, err)
	}
	if UsedBy([]Profile{{ID: "a", Account: "ops"}, {ID: "b"}, {ID: "c", Account: "ops"}}, "ops")[1] != "c" {
		t.Fatal("UsedBy")
	}
}
