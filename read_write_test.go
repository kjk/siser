package siser

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
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

func init() {
	genLargeValue()
	genSerializedSiser()
	genSerializedJSON()
}

func bufReaderFromBytes(d []byte) *bufio.Reader {
	return bufio.NewReader(bytes.NewBuffer(d))
}

func testRoundTrip(t *testing.T, recIn *Record) string {
	d := recIn.Marshal()
	rec, err := UnmarshalRecord(d, nil)
	assert.NoError(t, err)
	assert.Equal(t, rec.Entries, recIn.Entries)
	return string(d)
}

/*
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
	unixNano := 5 * time.Second
	tm := time.Unix(0, int64(unixNano))
	for i, s := range strings {
		name := names[i]
		_, err = w.WriteNamed([]byte(s), tm, name)
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
*/

func TestRecordSerializeSimple(t *testing.T) {
	var r Record
	r.Append("key", "val")
	s := testRoundTrip(t, &r)
	assert.Equal(t, "key: val\n", s)
}

func TestRecordSerializeSimple2(t *testing.T) {
	var r Record
	r.Append("k2", "a\nb")
	s := testRoundTrip(t, &r)
	assert.Equal(t, "k2:+3\na\nb\n", s)
}

func TestRecordSerializeSimple3(t *testing.T) {
	var r Record
	r.Append("long key", largeValue)
	got := testRoundTrip(t, &r)
	exp := fmt.Sprintf("long key:+%d\n%s\n", len(largeValue), largeValue)
	assert.Equal(t, exp, got)
}

/*
func testMany(t *testing.T, name string) {
	path := "test.txt"
	os.Remove(path)
	f, err := os.Create(path)
	assert.NoError(t, err)
	if err != nil {
		return
	}
	defer os.Remove(path)

	w := NewWriter(f)

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
		assert.Equal(t, positions[i], recPos)
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
*/

func TestAppendPanics(t *testing.T) {
	rec := &Record{}
	assert.Panics(t, func() { rec.Append("foo") }, "should panic with even number of arguments")
}

func TestIntStrLen(t *testing.T) {
	numbers := []int{-1, 0, 1}
	n1 := 1
	n2 := -1
	for i := 0; i < 10; i++ {
		n1 := n1*10 + i + 1
		numbers = append(numbers, n1)
		n2 := n2*10 - i - 1
		numbers = append(numbers, n2)
	}
	for _, n := range numbers {
		got := intStrLen(n)
		exp := len(strconv.Itoa(n))
		assert.Equal(t, exp, got)
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

func BenchmarkSiserUnmarshal(b *testing.B) {
	var rec Record
	var err error
	for n := 0; n < b.N; n++ {
		err = rec.Unmarshal(serializedSiser)
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
