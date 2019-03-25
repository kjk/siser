package siser

import "fmt"

func fmtArgs(args ...interface{}) string {
	if len(args) == 0 {
		return ""
	}
	format := args[0].(string)
	if len(args) == 1 {
		return format
	}
	return fmt.Sprintf(format, args[1:]...)
}

func panicWithMsg(defaultMsg string, args ...interface{}) {
	s := fmtArgs(args...)
	if s == "" {
		s = defaultMsg
	}
	panic(s)
}

func panicIfErr(err error, args ...interface{}) {
	if err == nil {
		return
	}
	panicWithMsg(err.Error(), args...)
}

func panicIf(cond bool, args ...interface{}) {
	if !cond {
		return
	}
	panicWithMsg("fatalIf: condition failed", args...)
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
