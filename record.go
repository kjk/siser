package siser

import (
	"fmt"
	"strconv"
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
	r.Name = ""
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
