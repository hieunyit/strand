package backup

// Strand backup stream format "SBK1": optional streaming zstd compression and optional client-side
// AES-256-GCM encryption of a backup payload. Raw payloads (neither option) are stored byte-for-byte
// without a header so native tools can read them directly.
//
//	header  := magic "STRNDBK1" | flags u8 | chunk u32 BE | keyIDLen u8 | keyID | [salt 32 | keycheck 8]
//	body    := zstd(plaintext)                       when only compressed
//	         | frame* (last frame flagged)           when encrypted (of the compressed bytes if compressed)
//	frame   := len u32 BE | AES-256-GCM(streamKey, nonce = counter u88 BE | last u8, aad = header)
//
// streamKey = HKDF-SHA256(masterKey, salt, "strand-backup-v1 aes-256-gcm"): a fresh random salt per
// stream gives every backup its own key, so counter nonces never repeat under one key. The flagged last
// frame (STREAM construction) makes truncation and frame reordering detectable; data after it is
// rejected. keycheck = HMAC-SHA256(streamKey, "strand-backup-keycheck")[:8] gives a clear "wrong key"
// error before any frame is decrypted.

import (
	"bufio"
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"runtime"

	"github.com/klauspost/compress/zstd"
)

const (
	streamMagic     = "STRNDBK1"
	flagZstd        = 1 << 0
	flagAESGCM      = 1 << 1
	defaultChunk    = 1 << 20
	maxChunk        = 16 << 20
	saltLen         = 32
	keyCheckLen     = 8
	hkdfInfo        = "strand-backup-v1 aes-256-gcm"
	keyCheckLabel   = "strand-backup-keycheck"
	FormatRaw       = "raw"
	FormatSBK1      = "sbk1"
	CompressionZstd = "zstd"
)

var (
	ErrWrongKey     = errors.New("backup stream: wrong encryption key")
	ErrUnknownKey   = errors.New("backup stream: encryption key is not configured")
	ErrCorrupt      = errors.New("backup stream: corrupt or tampered data")
	ErrTruncated    = errors.New("backup stream: truncated (final frame missing)")
	ErrNotEncrypted = errors.New("backup stream: not an SBK1 stream")
)

// EncodeOptions selects the transformations applied by NewEncoder.
type EncodeOptions struct {
	Compress bool
	// Level is the zstd level (1 fastest .. 4 best; 0 = default 2).
	Level int
	// Key encrypts the stream when non-nil.
	Key *Key
	// ChunkSize is the encrypted frame size (default 1 MiB).
	ChunkSize int
}

// Format reports the catalog format for the options.
func (o EncodeOptions) Format() string {
	if o.Compress || o.Key != nil {
		return FormatSBK1
	}
	return FormatRaw
}

// StreamStats describes the plaintext seen by an encoder or decoder.
type StreamStats struct {
	PlainBytes  int64
	PlainSHA256 string
}

// Encoder transforms plaintext written to it into the stored representation written to dst.
type Encoder struct {
	dst     io.Writer
	plain   hash.Hash
	n       int64
	zw      *zstd.Encoder
	enc     *frameWriter
	sink    io.Writer // where (possibly compressed) bytes go
	closed  bool
	raw     bool
	errLast error
}

// NewEncoder writes the stream header (unless raw) and returns a plaintext writer. Close must be called
// to flush the final frame; it does not close dst.
func NewEncoder(dst io.Writer, opt EncodeOptions) (*Encoder, error) {
	e := &Encoder{dst: dst, plain: sha256.New()}
	if !opt.Compress && opt.Key == nil {
		e.raw, e.sink = true, dst
		return e, nil
	}
	chunk := opt.ChunkSize
	if chunk <= 0 {
		chunk = defaultChunk
	}
	if chunk > maxChunk {
		return nil, fmt.Errorf("backup stream: chunk size %d exceeds %d", chunk, maxChunk)
	}
	var flags byte
	if opt.Compress {
		flags |= flagZstd
	}
	header := new(bytes.Buffer)
	header.WriteString(streamMagic)
	var streamKey []byte
	if opt.Key != nil {
		if err := opt.Key.validate(); err != nil {
			return nil, err
		}
		flags |= flagAESGCM
	}
	header.WriteByte(flags)
	_ = binary.Write(header, binary.BigEndian, uint32(chunk))
	keyID := ""
	if opt.Key != nil {
		keyID = opt.Key.ID
	}
	header.WriteByte(byte(len(keyID)))
	header.WriteString(keyID)
	if opt.Key != nil {
		salt := make([]byte, saltLen)
		if _, err := rand.Read(salt); err != nil {
			return nil, err
		}
		var err error
		streamKey, err = deriveStreamKey(opt.Key.Secret, salt)
		if err != nil {
			return nil, err
		}
		header.Write(salt)
		header.Write(keyCheck(streamKey))
	}
	if _, err := dst.Write(header.Bytes()); err != nil {
		return nil, err
	}
	e.sink = dst
	if opt.Key != nil {
		fw, err := newFrameWriter(dst, streamKey, header.Bytes(), chunk)
		if err != nil {
			return nil, err
		}
		e.enc, e.sink = fw, fw
	}
	if opt.Compress {
		level := zstd.SpeedDefault
		switch opt.Level {
		case 1:
			level = zstd.SpeedFastest
		case 3:
			level = zstd.SpeedBetterCompression
		case 4:
			level = zstd.SpeedBestCompression
		}
		zw, err := zstd.NewWriter(e.sink, zstd.WithEncoderLevel(level), zstd.WithEncoderConcurrency(runtime.GOMAXPROCS(0)))
		if err != nil {
			return nil, err
		}
		e.zw = zw
	}
	return e, nil
}

func (e *Encoder) Write(p []byte) (int, error) {
	if e.closed {
		return 0, errors.New("backup stream: write after close")
	}
	e.plain.Write(p)
	e.n += int64(len(p))
	if e.zw != nil {
		return e.zw.Write(p)
	}
	return e.sink.Write(p)
}

// Close flushes compression and writes the final authenticated frame.
func (e *Encoder) Close() error {
	if e.closed {
		return e.errLast
	}
	e.closed = true
	if e.zw != nil {
		if err := e.zw.Close(); err != nil {
			e.errLast = err
			return err
		}
	}
	if e.enc != nil {
		e.errLast = e.enc.finish()
	}
	return e.errLast
}

// Stats returns the plaintext size and SHA-256 (valid after Close).
func (e *Encoder) Stats() StreamStats {
	return StreamStats{PlainBytes: e.n, PlainSHA256: hex.EncodeToString(e.plain.Sum(nil))}
}

// StreamHeader is the parsed SBK1 header.
type StreamHeader struct {
	Compressed bool
	Encrypted  bool
	KeyID      string
	ChunkSize  int
}

// Decoder reads the plaintext of a stored stream and verifies it while reading.
type Decoder struct {
	Header StreamHeader
	r      io.Reader
	zr     *zstd.Decoder
	plain  hash.Hash
	n      int64
	want   string
	wantN  int64
	done   bool
}

// DecodeOptions controls NewDecoder. Format "raw" passes bytes through.
type DecodeOptions struct {
	Format string
	Keys   *Keyring
	// PlainSHA256/PlainBytes, when set, are verified at EOF (the final Read returns an error instead of EOF).
	PlainSHA256 string
	PlainBytes  int64
}

// NewDecoder wraps src. For SBK1 it parses the header, checks the key and returns a plaintext reader.
func NewDecoder(src io.Reader, opt DecodeOptions) (*Decoder, error) {
	d := &Decoder{plain: sha256.New(), want: opt.PlainSHA256, wantN: -1}
	if opt.PlainSHA256 != "" {
		d.wantN = opt.PlainBytes
	}
	if opt.Format == "" || opt.Format == FormatRaw {
		d.r = src
		return d, nil
	}
	if opt.Format != FormatSBK1 {
		return nil, fmt.Errorf("backup stream: unsupported format %q", opt.Format)
	}
	br := bufio.NewReaderSize(src, 64<<10)
	header := new(bytes.Buffer)
	fixed := make([]byte, len(streamMagic)+1+4+1)
	if _, err := io.ReadFull(br, fixed); err != nil {
		return nil, fmt.Errorf("%w: header: %v", ErrCorrupt, err)
	}
	header.Write(fixed)
	if string(fixed[:len(streamMagic)]) != streamMagic {
		return nil, ErrNotEncrypted
	}
	flags := fixed[len(streamMagic)]
	if flags&^(flagZstd|flagAESGCM) != 0 {
		return nil, fmt.Errorf("%w: unknown flags %#x", ErrCorrupt, flags)
	}
	chunk := int(binary.BigEndian.Uint32(fixed[len(streamMagic)+1:]))
	keyIDLen := int(fixed[len(fixed)-1])
	keyID := make([]byte, keyIDLen)
	if _, err := io.ReadFull(br, keyID); err != nil {
		return nil, fmt.Errorf("%w: key id: %v", ErrCorrupt, err)
	}
	header.Write(keyID)
	d.Header = StreamHeader{Compressed: flags&flagZstd != 0, Encrypted: flags&flagAESGCM != 0, KeyID: string(keyID), ChunkSize: chunk}
	var body io.Reader = br
	if d.Header.Encrypted {
		if chunk <= 0 || chunk > maxChunk {
			return nil, fmt.Errorf("%w: invalid chunk size", ErrCorrupt)
		}
		tail := make([]byte, saltLen+keyCheckLen)
		if _, err := io.ReadFull(br, tail); err != nil {
			return nil, fmt.Errorf("%w: salt: %v", ErrCorrupt, err)
		}
		header.Write(tail)
		if opt.Keys == nil {
			return nil, fmt.Errorf("%w: backup is encrypted with key %q; configure STRAND_BACKUP_ENCRYPTION_KEYS", ErrUnknownKey, d.Header.KeyID)
		}
		k, ok := opt.Keys.Get(d.Header.KeyID)
		if !ok {
			return nil, fmt.Errorf("%w: backup is encrypted with key %q; configure STRAND_BACKUP_ENCRYPTION_KEYS", ErrUnknownKey, d.Header.KeyID)
		}
		streamKey, err := deriveStreamKey(k.Secret, tail[:saltLen])
		if err != nil {
			return nil, err
		}
		if !hmac.Equal(keyCheck(streamKey), tail[saltLen:]) {
			return nil, fmt.Errorf("%w for key id %q", ErrWrongKey, d.Header.KeyID)
		}
		fr, err := newFrameReader(br, streamKey, header.Bytes(), chunk)
		if err != nil {
			return nil, err
		}
		body = fr
	} else if keyIDLen != 0 {
		return nil, fmt.Errorf("%w: key id on an unencrypted stream", ErrCorrupt)
	}
	if d.Header.Compressed {
		zr, err := zstd.NewReader(body, zstd.WithDecoderConcurrency(0), zstd.WithDecoderMaxWindow(1<<30))
		if err != nil {
			return nil, err
		}
		d.zr, body = zr, zr
	}
	d.r = body
	return d, nil
}

func (d *Decoder) Read(p []byte) (int, error) {
	n, err := d.r.Read(p)
	if n > 0 {
		d.plain.Write(p[:n])
		d.n += int64(n)
	}
	if errors.Is(err, io.EOF) && !d.done {
		d.done = true
		if d.want != "" {
			if d.wantN >= 0 && d.n != d.wantN {
				return n, fmt.Errorf("%w: plaintext size %d, catalog says %d", ErrCorrupt, d.n, d.wantN)
			}
			if got := hex.EncodeToString(d.plain.Sum(nil)); got != d.want {
				return n, fmt.Errorf("%w: plaintext SHA-256 differs from the catalog", ErrCorrupt)
			}
		}
	}
	return n, err
}

// Close releases decoder resources (not the source).
func (d *Decoder) Close() error {
	if d.zr != nil {
		d.zr.Close()
	}
	return nil
}

// Stats returns plaintext bytes and SHA-256 read so far.
func (d *Decoder) Stats() StreamStats {
	return StreamStats{PlainBytes: d.n, PlainSHA256: hex.EncodeToString(d.plain.Sum(nil))}
}

func deriveStreamKey(master, salt []byte) ([]byte, error) {
	return hkdf.Key(sha256.New, master, salt, hkdfInfo, 32)
}

func keyCheck(streamKey []byte) []byte {
	m := hmac.New(sha256.New, streamKey)
	m.Write([]byte(keyCheckLabel))
	return m.Sum(nil)[:keyCheckLen]
}

func nonceFor(counter uint64, last bool) []byte {
	n := make([]byte, 12)
	binary.BigEndian.PutUint64(n[3:11], counter)
	if last {
		n[11] = 1
	}
	return n
}

type frameWriter struct {
	dst     io.Writer
	aead    cipher.AEAD
	aad     []byte
	buf     []byte
	chunk   int
	counter uint64
	out     []byte
}

func newFrameWriter(dst io.Writer, key, aad []byte, chunk int) (*frameWriter, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &frameWriter{dst: dst, aead: aead, aad: append([]byte(nil), aad...), chunk: chunk, buf: make([]byte, 0, chunk)}, nil
}

func (w *frameWriter) Write(p []byte) (int, error) {
	total := len(p)
	for len(p) > 0 {
		// A full buffer is sealed only when more data arrives, so the final frame is never empty unless
		// the whole stream is empty and the last frame is always flagged.
		if len(w.buf) == w.chunk {
			if err := w.seal(false); err != nil {
				return total - len(p), err
			}
		}
		n := copy(w.buf[len(w.buf):w.chunk], p)
		w.buf = w.buf[:len(w.buf)+n]
		p = p[n:]
	}
	return total, nil
}

func (w *frameWriter) seal(last bool) error {
	if w.counter == 1<<64-1 {
		return errors.New("backup stream: too many frames")
	}
	w.out = w.aead.Seal(w.out[:0], nonceFor(w.counter, last), w.buf, w.aad)
	w.counter++
	var l [4]byte
	binary.BigEndian.PutUint32(l[:], uint32(len(w.out)))
	if _, err := w.dst.Write(l[:]); err != nil {
		return err
	}
	_, err := w.dst.Write(w.out)
	w.buf = w.buf[:0]
	return err
}

func (w *frameWriter) finish() error { return w.seal(true) }

type frameReader struct {
	src     io.Reader
	aead    cipher.AEAD
	aad     []byte
	chunk   int
	counter uint64
	plain   []byte
	pos     int
	ct      []byte
	last    bool
}

func newFrameReader(src io.Reader, key, aad []byte, chunk int) (*frameReader, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &frameReader{src: src, aead: aead, aad: append([]byte(nil), aad...), chunk: chunk}, nil
}

func (r *frameReader) Read(p []byte) (int, error) {
	for r.pos >= len(r.plain) {
		if r.last {
			// Nothing may follow the final frame.
			var one [1]byte
			if n, _ := io.ReadFull(r.src, one[:]); n != 0 {
				return 0, fmt.Errorf("%w: data after the final frame", ErrCorrupt)
			}
			return 0, io.EOF
		}
		if err := r.next(); err != nil {
			return 0, err
		}
	}
	n := copy(p, r.plain[r.pos:])
	r.pos += n
	return n, nil
}

func (r *frameReader) next() error {
	var l [4]byte
	if _, err := io.ReadFull(r.src, l[:]); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return ErrTruncated
		}
		return err
	}
	size := int(binary.BigEndian.Uint32(l[:]))
	if size < r.aead.Overhead() || size > r.chunk+r.aead.Overhead() {
		return fmt.Errorf("%w: invalid frame length", ErrCorrupt)
	}
	if cap(r.ct) < size {
		r.ct = make([]byte, size)
	}
	r.ct = r.ct[:size]
	if _, err := io.ReadFull(r.src, r.ct); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return ErrTruncated
		}
		return err
	}
	plain, err := r.aead.Open(r.plain[:0], nonceFor(r.counter, false), r.ct, r.aad)
	if err != nil {
		plain, err = r.aead.Open(r.plain[:0], nonceFor(r.counter, true), r.ct, r.aad)
		if err != nil {
			return fmt.Errorf("%w: frame %d failed authentication", ErrCorrupt, r.counter)
		}
		r.last = true
	}
	r.counter++
	r.plain, r.pos = plain, 0
	return nil
}
