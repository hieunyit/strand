package backup

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func testKey(t *testing.T, id string) Key {
	t.Helper()
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return Key{ID: id, Secret: b}
}

func encode(t *testing.T, plain []byte, opt EncodeOptions) ([]byte, StreamStats) {
	t.Helper()
	var out bytes.Buffer
	e, err := NewEncoder(&out, opt)
	if err != nil {
		t.Fatal(err)
	}
	// Uneven writes exercise frame boundaries.
	for p := plain; len(p) > 0; {
		n := 1 + len(p)%7919
		if n > len(p) {
			n = len(p)
		}
		if _, err := e.Write(p[:n]); err != nil {
			t.Fatal(err)
		}
		p = p[n:]
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes(), e.Stats()
}

func decodeAll(stored []byte, opt DecodeOptions) ([]byte, error) {
	d, err := NewDecoder(bytes.NewReader(stored), opt)
	if err != nil {
		return nil, err
	}
	defer d.Close()
	return io.ReadAll(d)
}

func TestStreamRoundTripAllModes(t *testing.T) {
	k := testKey(t, "k1")
	ring, _ := NewKeyring(k)
	compressible := bytes.Repeat([]byte("INSERT INTO t VALUES (1,'hello world');\n"), 100000)
	random := make([]byte, 3<<20+17)
	_, _ = rand.Read(random)
	for _, tc := range []struct {
		name string
		opt  EncodeOptions
	}{
		{"raw", EncodeOptions{}},
		{"zstd", EncodeOptions{Compress: true}},
		{"aes", EncodeOptions{Key: &k, ChunkSize: 64 << 10}},
		{"zstd+aes", EncodeOptions{Compress: true, Key: &k}},
		{"zstd-best+aes", EncodeOptions{Compress: true, Level: 4, Key: &k}},
	} {
		for _, in := range [][]byte{nil, []byte("x"), compressible, random, bytes.Repeat([]byte{7}, 64<<10)} {
			stored, st := encode(t, in, tc.opt)
			sum := sha256.Sum256(in)
			if st.PlainBytes != int64(len(in)) || st.PlainSHA256 != hex.EncodeToString(sum[:]) {
				t.Fatalf("%s: stats %+v", tc.name, st)
			}
			if tc.opt.Format() == FormatRaw && !bytes.Equal(stored, in) {
				t.Fatalf("raw must be byte-for-byte")
			}
			if tc.opt.Key != nil && len(in) > 64 && bytes.Contains(stored, in[:64]) {
				t.Fatalf("%s: plaintext visible in ciphertext", tc.name)
			}
			got, err := decodeAll(stored, DecodeOptions{Format: tc.opt.Format(), Keys: ring, PlainSHA256: st.PlainSHA256, PlainBytes: st.PlainBytes})
			if err != nil || !bytes.Equal(got, in) {
				t.Fatalf("%s len %d: err=%v equal=%v", tc.name, len(in), err, bytes.Equal(got, in))
			}
		}
	}
	stored, _ := encode(t, compressible, EncodeOptions{Compress: true})
	if len(stored)*20 > len(compressible) {
		t.Fatalf("zstd ratio too low: %d -> %d", len(compressible), len(stored))
	}
}

func TestStreamWrongKeyRotationAndTamper(t *testing.T) {
	old := testKey(t, "2025-q4")
	cur := testKey(t, "2026-q1")
	plain := bytes.Repeat([]byte("secret row\n"), 50000)
	storedOld, st := encode(t, plain, EncodeOptions{Compress: true, Key: &old})

	// Rotation: the new key is active, the old one still decrypts old backups.
	ring, err := NewKeyring(cur, old)
	if err != nil {
		t.Fatal(err)
	}
	if ring.Active.ID != "2026-q1" {
		t.Fatal("first key must be active")
	}
	if got, err := decodeAll(storedOld, DecodeOptions{Format: FormatSBK1, Keys: ring}); err != nil || !bytes.Equal(got, plain) {
		t.Fatalf("old backup after rotation: %v", err)
	}
	// Key removed from the ring: clear error naming the key id.
	onlyNew, _ := NewKeyring(cur)
	if _, err := decodeAll(storedOld, DecodeOptions{Format: FormatSBK1, Keys: onlyNew}); !errors.Is(err, ErrUnknownKey) || !strings.Contains(err.Error(), "2025-q4") {
		t.Fatalf("missing key: %v", err)
	}
	if _, err := decodeAll(storedOld, DecodeOptions{Format: FormatSBK1}); !errors.Is(err, ErrUnknownKey) {
		t.Fatalf("no keyring: %v", err)
	}
	// Same id, different material: wrong key detected before decrypting frames.
	impostor := testKey(t, "2025-q4")
	bad, _ := NewKeyring(impostor)
	if _, err := decodeAll(storedOld, DecodeOptions{Format: FormatSBK1, Keys: bad}); !errors.Is(err, ErrWrongKey) {
		t.Fatalf("wrong key: %v", err)
	}
	// Bit flip in a frame body.
	flipped := append([]byte(nil), storedOld...)
	flipped[len(flipped)/2] ^= 1
	if _, err := decodeAll(flipped, DecodeOptions{Format: FormatSBK1, Keys: ring}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("tamper: %v", err)
	}
	// Header tamper (chunk size is authenticated as AAD).
	hdr := append([]byte(nil), storedOld...)
	hdr[len(streamMagic)+4] ^= 1
	if _, err := decodeAll(hdr, DecodeOptions{Format: FormatSBK1, Keys: ring}); err == nil {
		t.Fatal("header tamper accepted")
	}
	// Truncation at a frame boundary: the final flagged frame is missing.
	k := testKey(t, "t")
	r1, _ := NewKeyring(k)
	big := make([]byte, 5<<20)
	_, _ = rand.Read(big)
	stored, _ := encode(t, big, EncodeOptions{Key: &k, ChunkSize: 1 << 20})
	hlen := len(streamMagic) + 1 + 4 + 1 + 1 + saltLen + keyCheckLen
	frame := 4 + (1 << 20) + 16
	cut := stored[:hlen+2*frame]
	if _, err := decodeAll(cut, DecodeOptions{Format: FormatSBK1, Keys: r1}); !errors.Is(err, ErrTruncated) {
		t.Fatalf("truncated: %v", err)
	}
	// Trailing garbage after the final frame.
	if _, err := decodeAll(append(append([]byte(nil), stored...), 0), DecodeOptions{Format: FormatSBK1, Keys: r1}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("trailing data: %v", err)
	}
	// Catalog plaintext hash mismatch is reported at EOF.
	if _, err := decodeAll(storedOld, DecodeOptions{Format: FormatSBK1, Keys: ring, PlainSHA256: strings.Repeat("0", 64), PlainBytes: st.PlainBytes}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("plain hash mismatch: %v", err)
	}
}

func TestLoadKeyring(t *testing.T) {
	k1 := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32))
	k2 := hex.EncodeToString(bytes.Repeat([]byte{2}, 32))
	env := map[string]string{}
	get := func(k string) string { return env[k] }
	if r, err := LoadKeyring(get); r != nil || err != nil {
		t.Fatalf("empty env: %v %v", r, err)
	}
	env[EnvEncryptionKeys] = "new:" + k2 + ", old:" + k1
	r, err := LoadKeyring(get)
	if err != nil || r.Active.ID != "new" || len(r.IDs()) != 2 {
		t.Fatalf("keys: %v %+v", err, r)
	}
	env[EnvEncryptionKey] = k1
	env[EnvEncryptionKeyID] = "old"
	if _, err := LoadKeyring(get); err != nil {
		t.Fatalf("same id same material must merge: %v", err)
	}
	env[EnvEncryptionKey] = k2
	if _, err := LoadKeyring(get); err == nil || strings.Contains(err.Error(), k2) {
		t.Fatalf("conflict: %v", err)
	}
	delete(env, EnvEncryptionKey)
	env[EnvEncryptionKeys] = "short:" + base64.StdEncoding.EncodeToString([]byte("tooshort"))
	if _, err := LoadKeyring(get); err == nil || strings.Contains(err.Error(), "tooshort") {
		t.Fatalf("short key: %v", err)
	}
	// Key file: permissions are enforced.
	delete(env, EnvEncryptionKeys)
	dir := t.TempDir()
	path := filepath.Join(dir, "keys")
	if err := os.WriteFile(path, []byte("# rotation\nb:"+k2+"\na:"+k1+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	env[EnvEncryptionKeysFile] = path
	if _, err := LoadKeyring(get); err == nil || !strings.Contains(err.Error(), "chmod 600") {
		t.Fatalf("world-readable file accepted: %v", err)
	}
	_ = os.Chmod(path, 0o600)
	r, err = LoadKeyring(get)
	if err != nil || r.Active.ID != "b" {
		t.Fatalf("file: %v", err)
	}
	if fp := r.Active.Fingerprint(); len(fp) != 12 || strings.Contains(k2, fp) {
		t.Fatalf("fingerprint %q", fp)
	}
}

func BenchmarkEncodeZstdAES(b *testing.B) {
	k := Key{ID: "bench", Secret: bytes.Repeat([]byte{9}, 32)}
	data := bytes.Repeat([]byte("2026-11-20 12:00:00,order-123456,customer-42,EUR,199.99,shipped\n"), 1<<16)
	for _, tc := range []struct {
		name string
		opt  EncodeOptions
	}{{"zstd", EncodeOptions{Compress: true}}, {"aes", EncodeOptions{Key: &k}}, {"zstd+aes", EncodeOptions{Compress: true, Key: &k}}} {
		b.Run(tc.name, func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			for i := 0; i < b.N; i++ {
				e, _ := NewEncoder(io.Discard, tc.opt)
				_, _ = e.Write(data)
				_ = e.Close()
			}
		})
	}
}
