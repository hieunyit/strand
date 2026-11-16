package store

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
)

// Box seals secrets (.env content) with AES-256-GCM. Key: 64 hex chars or base64 of 32 bytes (used as is),
// otherwise a passphrase of at least 16 characters (key = sha256(passphrase)). The pipeline name is bound as
// additional data so a ciphertext cannot be moved to another row.
type Box struct{ aead cipher.AEAD }

const sealPrefix = "v1:"

// NewBox builds a Box from STRAND_WEB_SECRET_KEY.
func NewBox(secret string) (*Box, error) {
	var key []byte
	if b, err := hex.DecodeString(secret); err == nil && len(b) == 32 {
		key = b
	} else if b, err := base64.StdEncoding.DecodeString(secret); err == nil && len(b) == 32 {
		key = b
	} else if len(secret) >= 16 {
		h := sha256.Sum256([]byte(secret))
		key = h[:]
	} else {
		return nil, errors.New("STRAND_WEB_SECRET_KEY is too short: use at least 16 characters (recommended: `openssl rand -hex 32`)")
	}
	blk, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	a, err := cipher.NewGCM(blk)
	if err != nil {
		return nil, err
	}
	return &Box{aead: a}, nil
}

// Seal encrypts plain; aad is typically the pipeline name.
func (b *Box) Seal(plain []byte, aad string) ([]byte, error) {
	nonce := make([]byte, b.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	out := append([]byte(sealPrefix), nonce...)
	return b.aead.Seal(out, nonce, plain, []byte(aad)), nil
}

// Open decrypts the output of Seal.
func (b *Box) Open(sealed []byte, aad string) ([]byte, error) {
	n := b.aead.NonceSize()
	if len(sealed) < len(sealPrefix)+n || string(sealed[:len(sealPrefix)]) != sealPrefix {
		return nil, errors.New("sealed value has an unknown format")
	}
	nonce := sealed[len(sealPrefix) : len(sealPrefix)+n]
	plain, err := b.aead.Open(nil, nonce, sealed[len(sealPrefix)+n:], []byte(aad))
	if err != nil {
		return nil, fmt.Errorf("decrypt failed (wrong STRAND_WEB_SECRET_KEY?): %w", err)
	}
	return plain, nil
}
