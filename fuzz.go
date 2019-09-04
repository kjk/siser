// +build gofuzz

package siser

func Fuzz(d []byte) int {
	_, err := UnmarshalRecord(d)
	if err == nil {
		// bump priority of valid test cases
		return 1
	}
	return 0
}
