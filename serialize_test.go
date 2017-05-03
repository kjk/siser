package siser

import (
	"bufio"
	"bytes"
	"fmt"
	"math/rand"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	largeValue = ""
)

func init() {
	s := "0123456789"
	s += s // 20
	s += s // 40
	s += s // 80
	s += s // 160
	s += s // 320
	largeValue = s
}

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
}

func TestRecordSerializeSimple2(t *testing.T) {
	var r Record
	r = r.Append("k2", "a\nb")
	s := testRoundTrip(t, r)
	assert.Equal(t, "k2:+3\na\nb\n---\n", s)
}

func TestRecordSerializeSimple3(t *testing.T) {
	var r Record
	r = r.Append("long key", largeValue)
	got := testRoundTrip(t, r)
	exp := fmt.Sprintf("long key:+%d\n%s\n---\n", len(largeValue), largeValue)
	assert.Equal(t, exp, got)
}

func TestMany(t *testing.T) {
	w := &bytes.Buffer{}
	var rec Record
	for i := 0; i < 200; i++ {
		rec = rec.Reset()
		nRand := rand.Intn(1024)
		rec = rec.Append("counter", strconv.Itoa(i), "random", strconv.Itoa(nRand))
		if i%12 == 0 {
			rec = rec.Append("large", largeValue)
			// test a case where large value is last in the record as well
			// as being followed by another value
			if rand.Intn(1024) > 512 {
				rec = rec.Append("after", "whatever")
			}
		}
		_, err := w.Write(rec.Marshal())
		assert.Nil(t, err)
	}

	r := bytes.NewBuffer(w.Bytes())
	reader := NewReader(r)
	i := 0
	for reader.ReadNext() {
		rec = reader.Record()
		counter, ok := rec.Get("counter")
		assert.True(t, ok)
		exp := strconv.Itoa(i)
		assert.Equal(t, exp, counter)
		_, ok = rec.Get("random")
		assert.True(t, ok)
		i++
	}
}
