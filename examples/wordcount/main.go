package main

import (
	"encoding/binary"
	"log"
	"strings"

	"github.com/zariel/mapreduce"
)

type wordCount struct{}

func (*wordCount) Map(output mapreduce.MapOutputCollector, key, value []byte) error {
	// key is a filename value is a line
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

func main() {
	wc := &wordCount{}
	mr := mapreduce.New(wc, nil, "testdata/data.txt")

	if err := mr.Run(); err != nil {
		log.Fatal(err)
	}
}
