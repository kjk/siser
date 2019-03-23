package siser

import (
	"io"
	"strconv"
)

// Format describes the format of written records
type Format int

const (
	// FormatSeparator uses "---\n" at end of record for
	// separating records. Only applies when writing records
	FormatSeparator Format = iota
	// FormatSizePrefix uses "135 name\n" header before each record
	// where "135" is size and "name" is optional name of the record
	FormatSizePrefix
)

// Writer writes records to in a structured format
type Writer struct {
	Format Format
	w      io.Writer
}

// NewWriter creates a writer
func NewWriter(w io.Writer) *Writer {
	return &Writer{
		w:      w,
		Format: FormatSizePrefix,
	}
}

// WriteRecord writes a record in a specified format
func (w *Writer) WriteRecord(r *Record) (int, error) {
	r.noSeparator = (w.Format == FormatSizePrefix)
	d := r.Marshal()
	if w.Format == FormatSizePrefix {
		return w.WriteNamed(d, r.Name)
	}
	// if we have separator, name is ignored
	return w.w.Write(d)
}

// Write writes a block
func (w *Writer) Write(d []byte) (int, error) {
	return w.WriteNamed(d, "")
}

// WriteNamed writes a block with a name
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
