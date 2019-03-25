package siser

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"
	"time"
)

// Reader is for reading (deserializing) records from a bufio.Reader
type Reader struct {
	r *bufio.Reader

	// Record is available after ReadNextRecord() and over-written
	// in next ReadNextRecord()
	Record *Record

	// Data / Name / Timestampe are available after calling ReadNextData
	Data      []byte
	Name      string
	Timestamp time.Time

	// position of the current record within the reader.
	// We keep track of it so that callers can index records
	// by offset and seek to it
	CurrPos int64

	currStreamPos int64
	err           error
	done          bool
}

// NewReader creates a new reader
func NewReader(r *bufio.Reader) *Reader {
	return &Reader{
		r:      r,
		Record: &Record{},
	}
}

func (r *Reader) Done() bool {
	return r.err != nil || r.done
}

// ReadNextData reads next block from the reader, returns false
// when no more record. If returns false, check Err() to see
// if there were errors.
// After reading Data containst data, and Timestamp and (optional) Name
// contain meta-data
func (r *Reader) ReadNextData() bool {
	if r.Done() {
		return false
	}
	r.Name = ""
	r.CurrPos = r.currStreamPos

	// read header in the format:
	// "${size} ${timestamp_in_unix_epoch_ms} ${name}\n"
	// ${name} is optional
	hdr, err := r.r.ReadBytes('\n')
	if err != nil {
		if err == io.EOF {
			r.done = true
		} else {
			r.err = err
		}
		return false
	}
	r.currStreamPos += int64(len(hdr))
	rest := hdr[:len(hdr)-1] // remove '\n' from end
	idx := bytes.IndexByte(rest, ' ')
	if idx == -1 {
		r.err = fmt.Errorf("Unxpected header '%s'", string(hdr))
		return false
	}
	dataSize := rest[:idx]
	rest = rest[idx+1:]
	var name []byte
	var timestamp []byte
	idx = bytes.IndexByte(rest, ' ')
	if idx == -1 {
		// no nmae, just timestamp
		timestamp = rest
	} else {
		// timestamp and name
		timestamp = rest[:idx]
		name = rest[idx+1:]
	}

	size, err := strconv.ParseInt(string(dataSize), 10, 64)
	if err != nil {
		r.err = fmt.Errorf("Unxpected header '%s'", string(hdr))
		return false
	}

	timeMs, err := strconv.ParseInt(string(timestamp), 10, 64)
	if err != nil {
		r.err = fmt.Errorf("Unxpected header '%s'", string(hdr))
		return false
	}
	r.Timestamp = time.Unix(0, timeMs*10e6)
	r.Name = string(name)

	// we try to re-use r.Data as long as it doesn't grow too much
	// (limit to 1 MB)
	if cap(r.Data) > 1024*1024 {
		r.Data = nil
	}
	if size > int64(cap(r.Data)) {
		r.Data = make([]byte, size, size)
	} else {
		// re-use existing buffer
		r.Data = r.Data[:size]
	}
	n, err := io.ReadFull(r.r, r.Data)
	if err != nil {
		r.err = err
		return false
	}
	panicIf(n != len(r.Data))
	r.currStreamPos += int64(n)

	// account for the fact that for readability we might
	// have padded data with '\n'
	d, err := r.r.Peek(1)
	if len(d) > 0 {
		if d[0] == '\n' {
			_, err = r.r.ReadByte()
			r.currStreamPos++
		}
	} else {
		if err == io.EOF {
			err = nil
		}
	}
	if err != nil {
		r.err = err
		return false
	}
	return true
}

// ReadNextData reads a block of data. Returns false if there are
// no more record (in which case check Err() for errors).
// After reading Record is avilable
func (r *Reader) ReadNextRecord() bool {
	done := r.ReadNextData()
	if done {
		return false
	}

	_, r.err = UnmarshalRecord(r.Data, r.Record)
	if r.err != nil {
		return false
	}
	r.Record.Name = r.Name
	r.Record.Timestamp = r.Timestamp
	return true
}

// Err returns error from last Read. We swallow io.EOF to make it easier
// to use
func (r *Reader) Err() error {
	return r.err
}
