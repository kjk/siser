package siser

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

/*
Serialize/Deserialize array of key/value pairs in a format that is easy
to serialize/parse and human-readable.

The basic format is line-oriented: "key: value\n"

When value is long (> 120 chars) or has \n in it, we serialize it as:
key:+$len\n
value\n

Records are separated by "---\n""
*/

const (
	recordSeparator = "---"
)

// Record represents list of key/value pairs that can
// be serialized/descerialized
type Record []string

// Append adds key/value pairs to a record
func (r Record) Append(args ...string) Record {
	fatalIf(len(args) == 0, "Append requires multiple arguments")
	fatalIf(len(args)%2 != 0, "Append requires even number of arguments")
	return append(r, args...)
}

// Reset makes it easy to re-use Record (as opposed to allocating a new one
// each time)
func (r Record) Reset() Record {
	if len(r) == 0 {
		// r can be nil
		return r
	}
	return r[0:0]
}

func isASCII(s string) bool {
	n := len(s)
	for i := 0; i < n; i++ {
		b := s[i]
		if b < 32 || b > 127 {
			return false
		}
	}
	return true
}

// Marshal converts to a byte array
func (r Record) Marshal() []byte {
	n := len(r)
	if n == 0 {
		return nil
	}
	var lines []string
	for i := 0; i < n/2; i++ {
		key := r[i*2]
		val := r[i*2+1]
		asData := len(val) > 120 || !isASCII(val)
		var l string
		if asData {
			l = fmt.Sprintf("%s:+%d\n%s", key, len(val), val)
		} else {
			l = fmt.Sprintf("%s: %s", key, val)
		}
		lines = append(lines, l)
	}
	lines = append(lines, recordSeparator)
	s := strings.Join(lines, "\n") + "\n"
	return []byte(s)
}

// Reader is for reading (deserializing) records
// from io.Reader
type Reader struct {
	r   io.Reader
	br  *bufio.Reader
	rec Record
	err error
}

// NewReader creates a new reader
func NewReader(r io.Reader) *Reader {
	return &Reader{
		r:  r,
		br: bufio.NewReader(r),
	}
}

// Read reads next record from the reader, returns false
// when no more records (error or reached end of file)
func (r *Reader) Read() bool {
	r.rec, r.err = ReadRecord(r.br, r.rec)
	if r.rec != nil {
		fatalIfErr(r.err)
		return true
	}
	return false
}

// Record returns record from last Read
func (r *Reader) Record() Record {
	return r.rec
}

// Err returns error from last Read. We swallow io.EOF to make it easier
// to use
func (r *Reader) Err() error {
	if r.err == io.EOF {
		return nil
	}
	return r.err
}

// ReadRecord reads another record from io.Reader
// If error is io.EOF, there are no more records in the reader
// We need bufio.Reader here for efficient reading of lines
// with occasional reads of raw bytes.
// Record is passed in so that it can be re-used
func ReadRecord(r *bufio.Reader, rec Record) (Record, error) {
	var line string
	rec = rec.Reset()
	var err error
	for {
		line, err = r.ReadString('\n')
		if err == io.EOF {
			if len(rec) > 0 {
				return nil, fmt.Errorf("half-read records %v", rec)
			}
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		n := len(line)
		if n < 3 {
			return nil, fmt.Errorf("line in unrecognized format: '%s'", line)
		}
		line = line[:n-1]
		if line == recordSeparator {
			return rec, nil
		}
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("line in unrecognized format: '%s'", line)
		}
		key := parts[0]
		val := parts[1]
		if len(val) < 1 {
			return nil, fmt.Errorf("line in unrecognized format: '%s'", line)
		}
		typ := val[0]
		val = val[1:]
		if typ == ' ' {
			rec = rec.Append(key, val)
			continue
		}

		if typ != '+' {
			return nil, fmt.Errorf("line in unrecognized format: '%s'", line)
		}
		n, err := strconv.Atoi(val)
		if err != nil {
			return nil, err
		}
		// account for '\n'
		n++
		d := make([]byte, n, n)
		n, err = r.Read(d)
		if err != nil {
			return nil, err
		}
		if n != len(d) {
			return nil, fmt.Errorf("wanted to read %d but read %d bytes", len(d), n)
		}
		val = string(d[:n-1])
		rec = rec.Append(key, val)
	}
}
