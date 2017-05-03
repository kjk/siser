# siser

Package `siser` is a Simple Serialization library for Go

Imagine you want to write many records of somewhat structured data
to a file. Think of it as a helper for structured logging.

You could use csv format, but csv values are identified by a position,
not name.

You could serialize as json and write a line per json record, but
json isn't great for human readability (imagine you `tail -f` a log
file with json records).

This library is meant to be a middle ground:
* you can serialize arbitrary records with multiple key/value pairs
* the output is human-readable
* it's desgined to be efficient and simple to use

## API usage

Imagine you want log basic info about http requests.

```go
var r siser.Record
func logHTTPRequest(w io.Writer, url string, ipAddr string, statusCode int) error {
  // for efficiency, you can re-use siser.Record across calls
  r = r.Reset()
  // you can append multiple key/value paris ot once
  r = r.Append("url", url, "ipaddr", ipAddr)
  // or spread over multiple parts
  r = r.Append("code", strconv.Itoa(statusCode))
  d := r.Marshal()
  _, err := w.Write(d)
  return err
}
```

The data will be serialized as following:
```
url: http://blog.kowalczyk.info/index.html
ipaddr: 10.0.0.1
code: 200
---
```

Re-inventing the wheel, I know, there are existing formats for logging
http requests. The good thing about this format is that it can be used
for arbitrary data that can be represented as key/value pairs

We also need to read the data back. Let's assume you wrote the data to
a file `my_data.txt`. To read all records from the file:
```go
f, err := os.Open("my_data.txt")
fatalIfErr(err)
defer f.Close()
r := siser.NewReader(f)
for r.Read() {
  record := r.Record()
  // do something with the data
}
fatalIfErr(r.Err())
```

## Usage scenarios

I use `siser` for in my web services for 2 use cases:

* logging to help in debugging issues after they happen
* implementing poor-man's analytics

Logging for debugging adds a little bit more structure over
adhoc logging. I can add some meta-data to log entries
and in addition to reading the logs I can quickly write
programs that filter the logs. For example if I add serving time
to http request log I could easily write a program that shows
requests that take over 1 second to serve.

Another one is poor-man's analytics. For example, if you're building
a web service that converts .png file to .ico file, it would be
good to know daily statistics about how many files were converted,
how much time an average conversion takes etc.

## Performance and implementation notes

I'm not doing anything crazy. The format is designed to be simple
to implement in efficient way. Howerver, some implementation decisions
were made with performance in mind.

`siser.Record` is an alias for `[]string`. You might expect this to
be `map[string]string` but `[]string` is more efficient for small
number of entries.

This is why you have to use slice-like `r = r.Append("key", "val")` pattern.

`siser.Record` can also be re-used across calls to `Marshal`. Thanks to
using a slice we can have `r = r.Reset()` call which re-uses underlying
array. Imagine you serialize 1.000 records. With re-use you only allocate
one `[]string` slice compared to making 1.000 allocations.

`siser.Reader` takes advantage for this optimization when reading
multiple records from a file.

The format is binary-safe and works for serializing large values e.g.
you can use png image as value.

The value is a string but in Go a string can have arbitrary binary data
in it. A small (<120 bytes) ascii value is serialized as a single line:
```
${key}: ${value}\n
```

When value is large or has non-ascii characters, we use a slightly different
format:
```
${key}:+${len(value)}\
${value}\n
```
