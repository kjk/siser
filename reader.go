package siser

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// Reader is for reading (deserializing) records
// from io.Reader
type Reader struct {
	Format Format

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
		Format: FormatSeparator,
		r:      r,
		br:     bufio.NewReader(r),
	}
}

// ReadNext reads next record from the reader, returns false
// when no more records (error or reached end of file)
func (r *Reader) ReadNext() bool {
	if r.err != nil {
		return false
	}
	r.rec.noSeparator = (r.Format == FormatSizePrefix)
	var n int
	r.currRecPos = r.nextRecPos
	n, r.err = ReadRecord(r.br, &r.rec)
	r.nextRecPos += int64(n)
	if r.err != nil {
		return false
	}
	return true
}

// ReadNextData reads a block of data. Returns false if finished
// reading. Check Err() to see if there was an error reading.
// After reading r.Data and r.Name contains data and (optional) name.
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

// ReadSizePrefixed reads data in FormatSizePrefixed.
// Returns data, total number of bytes read (which is bigger than
// the size of data), optional name and error.
func ReadSizePrefixed(r *bufio.Reader) ([]byte, int, string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, 0, "", err
	}
	nBytesRead := len(line)
	// account for the fact that for readability we might
	// have padded a record with '\n' so here we might
	// get an empty line
	if len(line) == 1 {
		line, err = r.ReadString('\n')
		if err != nil {
			return nil, 0, "", err
		}
		nBytesRead += len(line)
	}
	// remove \n from the end
	line = line[:len(line)-1]
	var name string
	parts := strings.SplitN(line, " ", 2)
	size := parts[0]
	if len(parts) > 1 {
		name = parts[1]
	}
	n, err := strconv.Atoi(size)
	if err != nil {
		return nil, 0, "", err
	}
	nBytesRead += n
	d := make([]byte, n)
	_, err = r.Read(d[:])
	if err != nil {
		return nil, 0, "", err
	}
	return d, nBytesRead, name, nil
}

// ReadRecord reads another record from io.Reader
// If error is io.EOF, there are no more records in the reader
// We need bufio.Reader here for efficient reading of lines
// with occasional reads of raw bytes.
// Record is passed in so that it can be re-used
func ReadRecord(br *bufio.Reader, rec *Record) (int, error) {
	var line string
	rec.Reset()
	var err error
	r := br
	var nBytesRead2 int
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

	var nBytesRead int
	for {
		line, err = r.ReadString('\n')
		if err == io.EOF {
			if len(rec.Keys) != len(rec.Values) {
				return 0, fmt.Errorf("half-read record. keys: %#v, values: %#v", rec.Keys, rec.Values)
			}
			// in case of size prefixed records, io.EOF means end of parssing
			// current record, not the whole file
			if rec.noSeparator {
				err = nil
			}
			return nBytesRead2, err
		}
		if err != nil {
			return 0, err
		}
		n := len(line)
		nBytesRead += n
		if n < 3 || line[n-1] != '\n' {
			return 0, fmt.Errorf("line in unrecognized format: '%s'", line)
		}
		if !rec.noSeparator {
			if line == recordSeparatorWithNL {
				return nBytesRead, nil
			}
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
