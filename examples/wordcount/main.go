package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"strings"

	"github.com/zariel/MapReduce"
)

func init() {
	log.SetFlags(log.Lshortfile | log.LstdFlags)
}

type stringCountOF struct{}

func (*stringCountOF) RecordWriter(w io.Writer) MapReduce.RecordWriter {
	return &stringCountW{w}
}

type stringCountW struct {
	w io.Writer
}

func (sc *stringCountW) WriteRecord(k, v []byte) error {
	_, err := fmt.Fprintf(sc.w, "%s %d\n", string(k), binary.BigEndian.Uint64(v))
	return err
}

type wordCount struct{}

func (*wordCount) Map(output MapReduce.MapOutputCollector, key, value []byte) error {
	line := string(value)
	// int64 YOLO
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, 1)
	for _, word := range strings.Split(line, " ") {
		if err := output.Emit([]byte(word), buf); err != nil {
			return err
		}
	}

	return nil
}

func (*wordCount) Reduce(key []byte, values <-chan []byte) []byte {
	var n uint64
	for v := range values {
		n += binary.BigEndian.Uint64(v)
	}

	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, n)

	return buf
}

func main() {
	wc := &wordCount{}
	mr := MapReduce.New(wc, wc, "testdata/data.txt")
	mr.OutputFormat = &stringCountOF{}
	mr.OutputPath = "out/"

	if err := mr.Run(); err != nil {
		log.Fatal(err)
	}
}
