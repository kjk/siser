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
	recordSeparatorWithNL = "---\n"
)

// Record represents list of key/value pairs that can
// be serialized/descerialized
type Record struct {
	// Keys contains record keys
	Keys []string
	// Values contains value for corresponding key in Keys
	Values []string
	Name   string

	// this exists for backwards compatibility
	// by default false so we'll add separator
	noSeparator bool
}

// Append adds key/value pairs to a record
func (r *Record) Append(args ...string) {
	n := len(args)
	if n == 0 || n%2 != 0 {
		panic(fmt.Sprintf("Invalid number of args: %d", len(args)))
	}
	for i := 0; i < n; i += 2 {
		r.Keys = append(r.Keys, args[i])
		r.Values = append(r.Values, args[i+1])
	}
}

func resetStringArray(a []string) []string {
	if a == nil {
		return nil
	}
	n := len(a)
	// avoid unwanted retaining of large strings
	for i := 0; i < n; i++ {
		a[i] = ""
	}
	return a[0:0]
}

// Reset makes it easy to re-use Record (as opposed to allocating a new one
// each time)
func (r *Record) Reset() {
	r.Keys = resetStringArray(r.Keys)
	r.Values = resetStringArray(r.Values)
}

// Get returns a value for a given key
func (r *Record) Get(key string) (string, bool) {
	keys := r.Keys
	n := len(keys)
	for i := 0; i < n; i++ {
		if key == keys[i] {
			return r.Values[i], true
		}
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

// intStrLen calculates how long n would be when converted to a string
// i.e. equivalent of len(strconv.Itoa(n)) but faster
// Note: not used
func intStrLen(n int) int {
	l := 1 // count the last digit here
	if n < 0 {
		n = -n
		l = 2
	}
	if n <= 9 {
		return l
	}
	if n <= 99 {
		return l + 1
	}
	if n <= 999 {
		return l + 2
	}
	for n > 999 {
		l++
		n = n / 10
	}
	return l + 2
}

// Marshal converts to a byte array
func (r *Record) Marshal() []byte {
	keys := r.Keys
	vals := r.Values
	nValues := len(keys)
	if nValues == 0 {
		return nil
	}
	// calculate size of serialized data so that we can pre-allocate buffer
	n := 0
	for i := 0; i < nValues; i++ {
		key := keys[i]
		val := vals[i]
		asData := len(val) > 120 || !isASCII(val)

		n += len(key) + 2 // +2 for separator
		if asData {
			s := strconv.Itoa(len(val))
			n += len(s) + 1 // +1 for '\n'
		}
		n += len(val) + 1 // +1 for '\n'
	}
	addSeparator := !r.noSeparator
	if addSeparator {
		n += len(recordSeparatorWithNL) // +1 for '\n'
	}
	buf := make([]byte, n, n)

	pos := 0
	for i := 0; i < nValues; i++ {
		key := keys[i]
		val := vals[i]
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
	if addSeparator {
		copy(buf[pos:], recordSeparatorWithNL)
		pos += len(recordSeparatorWithNL)
	}
	panicIf(pos != n)
	return buf
}

type WriteStyle int

const (
	// uses "---\n" at end of record for separating records
	WriteStyleSeparator WriteStyle = iota
	// uses "135 name\n" header before each record where
	// "135" is size and "name" is optional name of the record
	WriteStyleSizePrefix
)

type Writer struct {
	WriteStyle WriteStyle
	w          io.Writer
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{
		w:          w,
		WriteStyle: WriteStyleSizePrefix,
	}
}

func (w *Writer) WriteRecord(r *Record) (int, error) {
	r.noSeparator = (w.WriteStyle == WriteStyleSizePrefix)
	d := r.Marshal()
	if w.WriteStyle == WriteStyleSizePrefix {
		return w.WriteNamed(d, r.Name)
	}
	// if we have separator, name is ignored
	return w.w.Write(d)
}

func (w *Writer) Write(d []byte) (int, error) {
	return w.WriteNamed(d, "")
}

func (w *Writer) WriteNamed(d []byte, name string) (int, error) {
	var header string
	if name == "" {
		header = strconv.Itoa(len(d)) + "\n"
	} else {
		header = strconv.Itoa(len(d)) + " " + name + "\n"
	}
	d2 := append([]byte(header), d...)
	n := len(d2)
	if d2[n-1] != '\n' {
		d2 = append(d2, '\n')
	}
	return w.w.Write(d2)
}

// Reader is for reading (deserializing) records
// from io.Reader
type Reader struct {
	WriteStyle WriteStyle

	r  io.Reader
	br *bufio.Reader

	// rec is used when calling ReadNext()
	rec Record

	// Data / Name is used when calling ReadNextData()
	Data []byte
	Name string

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
		// for backwards compatibility
		WriteStyle: WriteStyleSeparator,
		r:          r,
		br:         bufio.NewReader(r),
	}
}

// ReadNext reads next record from the reader, returns false
// when no more records (error or reached end of file)
func (r *Reader) ReadNext() bool {
	if r.err != nil {
		return false
	}
	r.rec.noSeparator = (r.WriteStyle == WriteStyleSizePrefix)
	var n int
	r.currRecPos = r.nextRecPos
	n, r.err = ReadRecord(r.br, &r.rec)
	r.nextRecPos += int64(n)
	if r.err != nil {
		return false
	}
	return true
}

func (r *Reader) ReadNextData() bool {
	if r.err != nil {
		return false
	}
	var n int
	r.currRecPos = r.nextRecPos
	r.Data, n, r.Name, r.err = ReadSizePrefixed(r.br)
	r.nextRecPos += int64(n)
	return r.err != nil
}

// Record returns information from last ReadNext. Returns offset of the record
// (offset starts at 0 when NewReader is called) and record itself
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

func ReadSizePrefixed(r *bufio.Reader) ([]byte, int, string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, 0, "", err
	}
	n := len(line)
	// account for the fact that for readability we might
	// have padded a record with '\n' so here we might
	// get an empty line
	if len(line) == 1 {
		line, err = r.ReadString('\n')
		if err != nil {
			return nil, 0, "", err
		}
		n += len(line)
	}
	line = line[:len(line)-1]
	var name string
	parts := strings.SplitN(line, " ", 2)
	size := parts[0]
	if len(parts) > 1 {
		name = parts[1]
	}
	n, err = strconv.Atoi(size)
	if err != nil {
		return nil, 0, "", err
	}
	d := make([]byte, n, n)
	_, err = r.Read(d[:])
	if err != nil {
		return nil, 0, "", err
	}
	return d, len(d) + 1, name, nil
}

// ReadRecord reads another record from io.Reader
// If error is io.EOF, there are no more records in the reader
// We need bufio.Reader here for efficient reading of lines
// with occasional reads of raw bytes.
// Record is passed in so that it can be re-used
func ReadRecord(br *bufio.Reader, rec *Record) (int, error) {
	var line string
	nBytesRead := 0
	nBytesRead2 := 0
	rec.Reset()
	var err error
	r := br
	if rec.noSeparator {
		d, n, name, err := ReadSizePrefixed(br)
		if err != nil {
			return 0, err
		}
		nBytesRead2 = n
		rec.Name = name
		buf := bytes.NewBuffer(d)
		r = bufio.NewReader(buf)
	}
	for {
		line, err = r.ReadString('\n')
		if err == io.EOF {
			if len(rec.Keys) > 0 {
				return 0, fmt.Errorf("half-read record %v", rec.Keys)
			}
			return 0, io.EOF
		}
		if err != nil {
			return 0, err
		}
		n := len(line)
		nBytesRead += n
		if n < 3 || line[n-1] != '\n' {
			return 0, fmt.Errorf("line in unrecognized format: '%s'", line)
		}
		if line == recordSeparatorWithNL {
			if rec.noSeparator {
				return nBytesRead2, nil
			}
			return nBytesRead, nil
		}
		// strip '\n' from the end
		line = line[:n-1]
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			return 0, fmt.Errorf("line in unrecognized format: '%s'", line)
		}
		key := parts[0]
		val := parts[1]
		if len(val) < 1 {
			return 0, fmt.Errorf("line in unrecognized format: '%s'", line)
		}
		typ := val[0]
		val = val[1:]
		if typ == ' ' {
			rec.Keys = append(rec.Keys, key)
			rec.Values = append(rec.Values, val)
			continue
		}

		if typ != '+' {
			return 0, fmt.Errorf("line in unrecognized format: '%s'", line)
		}
		n, err := strconv.Atoi(val)
		if err != nil {
			return 0, err
		}
		// account for '\n'
		n++
		d := make([]byte, n, n)
		n, err = io.ReadFull(r, d)
		nBytesRead += n
		if err != nil {
			return 0, err
		}
		if n != len(d) {
			return 0, fmt.Errorf("wanted to read %d but read %d bytes", len(d), n)
		}
		val = string(d[:n-1])
		rec.Keys = append(rec.Keys, key)
		rec.Values = append(rec.Values, val)
	}
}
