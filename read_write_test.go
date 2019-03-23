package siser

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var (
	largeValue      = ""
	serializedJSON  []byte
	serializedSiser []byte
)

func genLargeValue() {
	s := "0123456789"
	s += s // 20
	s += s // 40
	s += s // 80
	s += s // 160
	s += s // 320
	largeValue = s
}

func genSerializedSiser() {
	var rec Record
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
	serializedSiser = rec.Marshal()
}

func genSerializedJSON() {
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
	serializedJSON = d
}

func init() {
	genLargeValue()
	genSerializedSiser()
	genSerializedJSON()
}

func bufReaderFromBytes(d []byte) *bufio.Reader {
	r := bytes.NewBuffer(d)
	return bufio.NewReader(r)
}

func testRoundTrip(t *testing.T, recIn *Record) string {
	d := recIn.Marshal()
	br := bufReaderFromBytes(d)
	rec := Record{}
	_, err := ReadRecord(br, &rec)
	assert.NoError(t, err)
	assert.Equal(t, rec.Keys, recIn.Keys)
	assert.Equal(t, rec.Values, recIn.Values)
	return string(d)
}

func TestWriterSizePrefixed(t *testing.T) {
	testWriter(t, FormatSizePrefix)
}

func TestWriterSeparator(t *testing.T) {
	testWriter(t, FormatSeparator)
}

func testWriter(t *testing.T, format Format) {
	strings := []string{"hey\n", "ho"}
	names := []string{"", "with name"}
	expPrefix := `4
hey
2 with name
ho
`
	var err error
	buf := &bytes.Buffer{}
	w := NewWriter(buf)
	w.Format = format
	for i, s := range strings {
		name := names[i]
		if name == "" {
			_, err = w.Write([]byte(s))
		} else {
			_, err = w.WriteNamed([]byte(s), name)
		}
		assert.NoError(t, err)
	}
	s := buf.String()
	assert.Equal(t, expPrefix, s)
	buf = bytes.NewBufferString(expPrefix)

	r := NewReader(buf)
	r.Format = format
	n := 0
	for r.ReadNextData() {
		assert.Equal(t, strings[n], string(r.Data))
		assert.Equal(t, names[n], string(r.Name))
		n++
	}
	assert.NoError(t, r.Err())
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

func TestManySeparator(t *testing.T) {
	testMany(t, FormatSeparator, "")
}

func TestManySizePrefixed(t *testing.T) {
	testMany(t, FormatSizePrefix, "")
}

func TestManySizePrefixedNamed(t *testing.T) {
	testMany(t, FormatSizePrefix, "named")
}

func testMany(t *testing.T, format Format, name string) {
	path := "test.txt"
	os.Remove(path)
	f, err := os.Create(path)
	assert.NoError(t, err)
	if err != nil {
		return
	}
	defer os.Remove(path)

	w := NewWriter(f)
	w.Format = format

	rec := &Record{}
	var positions []int64
	var currPos int64
	nRecs := 8
	for i := 0; i < nRecs; i++ {
		rec.Reset()
		rec.Name = name
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
		n, err := w.WriteRecord(rec)
		assert.Nil(t, err)
		positions = append(positions, currPos)
		currPos += int64(n)
	}

	err = f.Close()
	assert.NoError(t, err)

	f, err = os.Open(path)
	assert.NoError(t, err)
	defer f.Close()

	reader := NewReader(f)
	reader.Format = format
	i := 0
	for reader.ReadNext() {
		recPos, rec := reader.Record()
		assert.True(t, true || positions[i] == recPos)
		counter, ok := rec.Get("counter")
		assert.True(t, ok)
		exp := strconv.Itoa(i)
		assert.Equal(t, exp, counter)
		_, ok = rec.Get("random")
		assert.True(t, ok)
		assert.Equal(t, rec.Name, name)
		i++
	}
	assert.NoError(t, reader.Err())
	assert.Equal(t, nRecs, i)
}

func TestAppendPanics(t *testing.T) {
	rec := &Record{}
	assert.Panics(t, func() { rec.Append("foo") }, "should panic with even number of arguments")
}

func TestIntStrLen(t *testing.T) {
	tests := []struct {
		n   int
		exp int
	}{
		{-1, 2},
		{-89, 3},
		{-319, 4},
		{-2341, 5},
		{0, 1},
		{9, 1},
		{10, 2},
		{34, 2},
		{99, 2},
		{100, 3},
		{823, 3},
		{999, 3},
		{1000, 4},
		{3241, 4},
		{9999, 4},
		{10000, 5},
		{99999, 5},
	}
	for _, test := range tests {
		got := intStrLen(test.n)
		assert.Equal(t, test.exp, got)
	}
}

func dumpRec(rec *Record) {
	d := rec.Marshal()
	fmt.Printf("%s", string(d))
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

var (
	lines []string
)

func BenchmarkSiserUnmarshal(b *testing.B) {
	var rec Record
	var err error
	for n := 0; n < b.N; n++ {
		r := bufio.NewReader(bytes.NewBuffer(serializedSiser))
		_, err = ReadRecord(r, &rec)
		panicIfErr(err)
	}
}

func BenchmarkJSONUnmarshal(b *testing.B) {
	var rec testRecJSON
	for n := 0; n < b.N; n++ {
		err := json.Unmarshal(serializedJSON, &rec)
		panicIfErr(err)
	}
}
