package siser

import (
	"bufio"
	"bytes"
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
type Record struct {
	data []string
	buf  bytes.Buffer
}

// Append adds key/value pairs to a record
func (r *Record) Append(args ...string) {
	if len(args) == 0 || len(args)%2 != 0 {
		panic(fmt.Sprintf("Invalid number of args: %d", len(args)))
	}
	r.data = append(r.data, args...)
}

// Reset makes it easy to re-use Record (as opposed to allocating a new one
// each time)
func (r *Record) Reset() {
	if r.data != nil {
		r.data = r.data[0:0]
	}
}

// Get returns a value for a given key
func (r *Record) Get(key string) (string, bool) {
	data := r.data
	n := len(data)
	for idx := 0; idx < n; {
		if key == data[idx] {
			return data[idx+1], true
		}
		idx += 2
	}
	return "", false
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

var (
	sepLargeVal = []byte{':', '+'}
	sepSmallVal = []byte{':', ' '}
)

// MarshalOld converts to a byte array
func (r *Record) MarshalOld() []byte {
	data := r.data
	n := len(data)
	if n == 0 {
		return nil
	}
	buf := r.buf
	buf.Truncate(0)
	for i := 0; i < n/2; i++ {
		key := data[i*2]
		val := data[i*2+1]
		asData := len(val) > 120 || !isASCII(val)
		buf.WriteString(key)
		if asData {
			buf.Write(sepLargeVal)
			buf.WriteString(strconv.Itoa(len(val)))
			buf.WriteByte('\n')
		} else {
			buf.Write(sepSmallVal)
		}
		buf.WriteString(val)
		buf.WriteByte('\n')

	}
	buf.WriteString(recordSeparator)
	buf.WriteString("\n")
	return buf.Bytes()
}

// Marshal converts to a byte array
func (r *Record) Marshal() []byte {
	data := r.data
	nValues := len(data)
	if nValues == 0 {
		return nil
	}
	n := 0
	for i := 0; i < nValues/2; i++ {
		key := data[i*2]
		val := data[i*2+1]
		asData := len(val) > 120 || !isASCII(val)

		n += len(key) + 2 // +2 for separator
		if asData {
			s := strconv.Itoa(len(val))
			n += len(s) + 1 // +1 for '\n'
		}
		n += len(val) + 1 // +1 for '\n'
	}
	n += len(recordSeparator) + 1 // +1 for '\n'

	buf := make([]byte, n, n)
	pos := 0
	for i := 0; i < nValues/2; i++ {
		key := data[i*2]
		val := data[i*2+1]
		asData := len(val) > 120 || !isASCII(val)
		copy(buf[pos:], key)
		pos += len(key)
		buf[pos] = ':'
		pos++
		if asData {
			buf[pos] = '+'
			pos++
			s := strconv.Itoa(len(val))
			copy(buf[pos:], s)
			pos += len(s)
			buf[pos] = '\n'
			pos++
		} else {
			buf[pos] = ' '
			pos++
		}
		copy(buf[pos:], val)
		pos += len(val)
		buf[pos] = '\n'
		pos++
	}
	buf[pos] = '-'
	buf[pos+1] = '-'
	buf[pos+2] = '-'
	buf[pos+3] = '\n'
	pos += 4
	panicIf(pos != n)
	return buf
}

// Reader is for reading (deserializing) records
// from io.Reader
type Reader struct {
	r   io.Reader
	br  *bufio.Reader
	rec Record
	err error
	// position of the current record within the reader. It will match
	// position within the reader if we start reading from the beginning
	// this is needed for cases where we want to seek to a given record
	currRecPos int64
	nextRecPos int64
}

// NewReader creates a new reader
func NewReader(r io.Reader) *Reader {
	return &Reader{
		r:  r,
		br: bufio.NewReader(r),
	}
}

// ReadNext reads next record from the reader, returns false
// when no more records (error or reached end of file)
func (r *Reader) ReadNext() bool {
	if r.err != nil {
		return false
	}
	var n int
	var dataTmp []string
	r.currRecPos = r.nextRecPos
	n, dataTmp, r.err = ReadRecord(r.br, &r.rec)
	r.nextRecPos += int64(n)
	if r.err != nil {
		return false
	}
	if dataTmp != nil {
		panicIf(n == 0, "n: %d", n)
		panicIfErr(r.err)
		return true
	}
	return false
}

// Record returns data from last Read
func (r *Reader) Record() (int64, *Record) {
	return r.currRecPos, &r.rec
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
func ReadRecord(r *bufio.Reader, record *Record) (int, []string, error) {
	var line string
	nBytesRead := 0
	record.Reset()
	data := record.data
	defer func() {
		record.data = data
	}()

	var err error
	for {
		line, err = r.ReadString('\n')
		if err == io.EOF {
			if len(data) > 0 {
				return 0, nil, fmt.Errorf("half-read record %v", data)
			}
			return 0, nil, nil
		}
		if err != nil {
			return 0, nil, err
		}
		n := len(line)
		nBytesRead += n
		if n < 3 || line[n-1] != '\n' {
			return 0, nil, fmt.Errorf("line in unrecognized format: '%s'", line)
		}
		// strip '\n' from the end
		line = line[:n-1]
		if line == recordSeparator {
			return nBytesRead, data, nil
		}
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			return 0, nil, fmt.Errorf("line in unrecognized format: '%s'", line)
		}
		key := parts[0]
		val := parts[1]
		if len(val) < 1 {
			return 0, nil, fmt.Errorf("line in unrecognized format: '%s'", line)
		}
		typ := val[0]
		val = val[1:]
		if typ == ' ' {
			data = append(data, key, val)
			continue
		}

		if typ != '+' {
			return 0, nil, fmt.Errorf("line in unrecognized format: '%s'", line)
		}
		n, err := strconv.Atoi(val)
		if err != nil {
			return 0, nil, err
		}
		// account for '\n'
		n++
		d := make([]byte, n, n)
		n, err = io.ReadFull(r, d)
		nBytesRead += n
		if err != nil {
			return 0, nil, err
		}
		if n != len(d) {
			return 0, nil, fmt.Errorf("wanted to read %d but read %d bytes", len(d), n)
		}
		val = string(d[:n-1])
		data = append(data, key, val)
	}
}
