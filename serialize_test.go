package siser

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

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

func testRoundTrip(t *testing.T, r *Record) string {
	d := r.Marshal()
	br := bufReaderFromBytes(d)
	rec := Record{}
	_, data, err := ReadRecord(br, &rec)
	assert.Nil(t, err)
	assert.Equal(t, r.data, data)
	return string(d)
}

func TestRecordSerializeSimple(t *testing.T) {
	var r Record
	r.Append("key", "val")
	s := testRoundTrip(t, &r)
	assert.Equal(t, "key: val\n---\n", s)
}

func TestRecordSerializeSimple2(t *testing.T) {
	var r Record
	r.Append("k2", "a\nb")
	s := testRoundTrip(t, &r)
	assert.Equal(t, "k2:+3\na\nb\n---\n", s)
}

func TestRecordSerializeSimple3(t *testing.T) {
	var r Record
	r.Append("long key", largeValue)
	got := testRoundTrip(t, &r)
	exp := fmt.Sprintf("long key:+%d\n%s\n---\n", len(largeValue), largeValue)
	assert.Equal(t, exp, got)
}

func TestMany(t *testing.T) {
	w := &bytes.Buffer{}
	rec := &Record{}
	var positions []int64
	var currPos int64
	for i := 0; i < 200; i++ {
		rec.Reset()
		nRand := rand.Intn(1024)
		rec.Append("counter", strconv.Itoa(i), "random", strconv.Itoa(nRand))
		if i%12 == 0 {
			rec.Append("large", largeValue)
			// test a case where large value is last in the record as well
			// as being followed by another value
			if rand.Intn(1024) > 512 {
				rec.Append("after", "whatever")
			}
		}
		n, err := w.Write(rec.Marshal())
		assert.Nil(t, err)
		positions = append(positions, currPos)
		currPos += int64(n)
	}

	r := bytes.NewBuffer(w.Bytes())
	reader := NewReader(r)
	i := 0
	var recPos int64
	for reader.ReadNext() {
		recPos, rec = reader.Record()
		assert.Equal(t, positions[i], recPos)
		counter, ok := rec.Get("counter")
		assert.True(t, ok)
		exp := strconv.Itoa(i)
		assert.Equal(t, exp, counter)
		_, ok = rec.Get("random")
		assert.True(t, ok)
		i++
	}
}

/*
uri: /atom.xml
code: 200
ip: 54.186.248.49
dur: 1.41
when: 2017-07-09T05:26:55Z
size: 35286
ua: Feedspot http://www.feedspot.com
referer: http://blog.kowalczyk.info/feed
*/

var rec Record
var globalData []byte

type testRecJSON struct {
	URI       string        `json:"uri"`
	Code      int           `json:"code"`
	IP        string        `json:"ip"`
	Duration  time.Duration `json:"dur"`
	When      time.Time     `json:"when"`
	Size      int           `json:"size"`
	UserAgent string        `json:"ua"`
	Referer   string        `json:"referer"`
}

func BenchmarkSiserMarshal(b *testing.B) {
	for n := 0; n < b.N; n++ {
		rec.Reset()
		rec.Append("uri", "/atom.xml")
		rec.Append("code", strconv.Itoa(200))
		rec.Append("ip", "54.186.248.49")
		durMs := float64(1.41) / float64(time.Millisecond)
		durStr := strconv.FormatFloat(durMs, 'f', 2, 64)
		rec.Append("dur", durStr)
		rec.Append("when", time.Now().Format(time.RFC3339))
		rec.Append("size", strconv.Itoa(35286))
		rec.Append("ua", "Feedspot http://www.feedspot.com")
		rec.Append("referer", "http://blog.kowalczyk.info/feed")
		// assign to global to prevents optimizing the loop
		globalData = rec.Marshal()
	}
}

func BenchmarkSiserMarshalOld(b *testing.B) {
	for n := 0; n < b.N; n++ {
		rec.Reset()
		rec.Append("uri", "/atom.xml")
		rec.Append("code", strconv.Itoa(200))
		rec.Append("ip", "54.186.248.49")
		durMs := float64(1.41) / float64(time.Millisecond)
		durStr := strconv.FormatFloat(durMs, 'f', 2, 64)
		rec.Append("dur", durStr)
		rec.Append("when", time.Now().Format(time.RFC3339))
		rec.Append("size", strconv.Itoa(35286))
		rec.Append("ua", "Feedspot http://www.feedspot.com")
		rec.Append("referer", "http://blog.kowalczyk.info/feed")
		// assign to global to prevents optimizing the loop
		globalData = rec.MarshalOld()
	}
}
func BenchmarkJSONMarshal(b *testing.B) {
	for n := 0; n < b.N; n++ {
		rec := testRecJSON{
			URI:       "/atom.xml",
			Code:      200,
			IP:        "54.186.248.49",
			Duration:  time.Microsecond * time.Duration(1410),
			When:      time.Now(),
			Size:      35286,
			UserAgent: "Feedspot http://www.feedspot.com",
			Referer:   "http://blog.kowalczyk.info/feed",
		}
		d, err := json.Marshal(rec)
		panicIfErr(err)
		// assign to global to prevents optimizing the loop
		globalData = d
	}
}
