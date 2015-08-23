package main

import (
	"encoding/binary"
	"log"
	"strings"

	"github.com/zariel/mapreduce"
)

func init() {
	log.SetFlags(log.Lshortfile | log.LstdFlags)
}

type wordCount struct{}

func (*wordCount) Map(output mapreduce.MapOutputCollector, key, value []byte) error {
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
	mr := mapreduce.New(wc, wc, "testdata/data.txt")

	if err := mr.Run(); err != nil {
		log.Fatal(err)
	}
}
