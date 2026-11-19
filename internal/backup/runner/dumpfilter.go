package runner

import (
	"bufio"
	"bytes"
	"io"
)

// gtidPurgedFilter comments out the `SET @@GLOBAL.GTID_PURGED=...;` statement of a mysqldump stream so the
// dump can be loaded into a server that already has GTIDs (a new database next to the source, or any
// non-empty server). The recorded GTID set stays in the catalog for point-in-time recovery.
type gtidPurgedFilter struct {
	r      *bufio.Reader
	out    bytes.Buffer
	inStmt bool
	err    error
}

// NewGTIDPurgedFilter wraps a mysqldump stream.
func NewGTIDPurgedFilter(r io.Reader) io.Reader {
	return &gtidPurgedFilter{r: bufio.NewReaderSize(r, 1<<20)}
}

var gtidPurgedPrefix = []byte("SET @@GLOBAL.GTID_PURGED=")

func (f *gtidPurgedFilter) Read(p []byte) (int, error) {
	for f.out.Len() == 0 && f.err == nil {
		line, err := f.r.ReadSlice('\n')
		if err == bufio.ErrBufferFull {
			// Very long line (bulk INSERT): pass through unchanged.
			f.out.Write(line)
			continue
		}
		if len(line) > 0 {
			trimmed := bytes.TrimLeft(line, " \t")
			switch {
			case f.inStmt:
				f.out.WriteString("-- ")
				f.out.Write(line)
				if bytes.Contains(line, []byte(";")) {
					f.inStmt = false
				}
			case bytes.HasPrefix(trimmed, gtidPurgedPrefix):
				f.out.WriteString("-- [strand] ")
				f.out.Write(line)
				f.inStmt = !bytes.Contains(line, []byte(";"))
			default:
				f.out.Write(line)
			}
		}
		if err != nil {
			f.err = err
		}
	}
	if f.out.Len() > 0 {
		return f.out.Read(p)
	}
	return 0, f.err
}

// StripGTIDPurged applies the filter to a whole dump.
func StripGTIDPurged(dump []byte) []byte {
	b, _ := io.ReadAll(NewGTIDPurgedFilter(bytes.NewReader(dump)))
	return b
}
