package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"

	"github.com/kjk/siser"
)

/*
This example shows how to log info about http requests.
*/

type HTTPRequestInfo struct {
	Method       string
	URL          string
	Status       int
	ResponseSize int64
	// there would be more info in real life
}

const (
	keyMethod       = "Method"
	keyURL          = "URL"
	keyStatus       = "Status"
	keyResponseSize = "Size"
)

func writeHTTPReq(httpLogFile *siser.Writer, i *HTTPRequestInfo) error {
	var r siser.Record
	r.Write(keyMethod, i.Method)
	r.Write(keyURL, i.URL)
	r.Write(keyStatus, strconv.Itoa(i.Status))
	r.Write(keyResponseSize, strconv.FormatInt(i.ResponseSize, 10))
	_, err := httpLogFile.WriteRecord(&r)
	return err
}

const (
	logFilePath = "http_log.txt"
)

func writeHTTPLog() error {
	f, err := os.Create(logFilePath)
	if err != nil {
		return err
	}
	defer func() {
		if f != nil {
			f.Close()
			os.Remove(logFilePath)
		}
	}()
	w := siser.NewWriter(f)
	{
		req := HTTPRequestInfo{
			Method:       "GET",
			URL:          "/",
			Status:       200,
			ResponseSize: 3484,
		}
		err = writeHTTPReq(w, &req)
		if err != nil {
			return err
		}
	}

	{
		req := HTTPRequestInfo{
			Method:       "POST",
			URL:          "/api/update",
			Status:       200,
			ResponseSize: 1834,
		}
		err = writeHTTPReq(w, &req)
		if err != nil {
			return err
		}
	}

	err = f.Sync()
	if err != nil {
		return err
	}
	err = f.Close()
	if err != nil {
		return err
	}
	f = nil
	st, err := os.Stat(logFilePath)
	if err == nil {
		fmt.Printf("Wrote 2 records to a file '%s' of size '%d'\n", logFilePath, st.Size())
	}
	return nil
}

func readHTTPLog() error {
	f, err := os.Open(logFilePath)
	if err != nil {
		return err
	}
	defer f.Close()
	br := bufio.NewReader(f)
	r := siser.NewReader(br)
	var urls []string
	for r.ReadNextRecord() {
		rec := r.Record
		uri, _ := rec.Get(keyURL)
		fmt.Printf("Current record position in the file: %d\n", r.CurrRecordPos)
		fmt.Printf("URL: '%s'\n\n", uri)
		urls = append(urls, uri)
	}
	if r.Err() != nil {
		return r.Err()
	}
	fmt.Printf("Read %d records. URLS: %#v\n", len(urls), urls)
	return nil
}

func removeHTTPLog() {
	os.Remove(logFilePath)
}

func testHTTPLog() {
	err := writeHTTPLog()
	if err != nil {
		fmt.Printf("writeHTTPLog() failed with '%s'\n", err)
		return
	}

	defer removeHTTPLog()
	fmt.Printf("\n")
	err = readHTTPLog()
	if err != nil {
		fmt.Printf("readHTTPLog() failed with '%s'\n", err)
	}
}
