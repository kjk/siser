package main

import (
	"flag"
)

var (
	flgTestHTTPLog bool
)

func parseFlags() {
	flag.BoolVar(&flgTestHTTPLog, "test-http-log", false, "test http log")
	flag.Parse()
}

func main() {
	parseFlags()

	if flgTestHTTPLog {
		testHTTPLog()
		return
	}

	flag.Usage()
}
