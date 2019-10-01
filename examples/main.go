package main

import (
	"flag"
	"fmt"
)

var (
	flgCreate bool
)

func parseFlags() {
	flag.BoolVar(&flgCreate, "create", false, "if true, creates")
	flag.Parse()
}

func doCreate() {
	fmt.Printf("doCrreate()\n")
}

func main() {
	parseFlags()

	if flgCreate {
		doCreate()
		return
	}

	flag.Usage()
}
