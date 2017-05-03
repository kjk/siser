package siser

import (
	"bufio"
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func bufReaderFromBytes(d []byte) *bufio.Reader {
	r := bytes.NewBuffer(d)
	return bufio.NewReader(r)
}

func testRoundTrip(t *testing.T, r Record) string {
	d := r.Marshal()
	br := bufReaderFromBytes(d)
	r2, err := ReadRecord(br, nil)
	assert.Nil(t, err)
	assert.Equal(t, r, r2)
	return string(d)
}

func TestRecordSerializeSimple(t *testing.T) {
	var r Record
	r = r.Append("key", "val")
	s := testRoundTrip(t, r)
	assert.Equal(t, "key: val\n---\n", s)

func TestRecordSerializeSimple2(t *testing.T) {
	var r Record
	r = r.Append("k2", "a\nb")
	s := testRoundTrip(t, r)
	assert.Equal(t, "k2:+3\na\nb\n---\n", s)
}

func TestRecordSerializeSimple3(t *testing.T) {
	var r Record
	s := "0123456789"
	s += s // 20
	s += s // 40
	s += s // 80
	s += s // 160
	s += s // 320
	r = r.Append("long key", s)
	got := testRoundTrip(t, r)
	exp := fmt.Sprintf("long key:+%d\n%s\n---\n", len(s), s)
	assert.Equal(t, exp, got)
}
