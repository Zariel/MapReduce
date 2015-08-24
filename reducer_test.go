package MapReduce

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"

	"testing"
)

type reducerFunc func(key []byte, values <-chan []byte) []byte

func (fn reducerFunc) Reduce(key []byte, values <-chan []byte) []byte {
	return fn(key, values)
}

type mapRecordWriter struct {
	m map[string]int
}

func (m *mapRecordWriter) WriteRecord(key, value []byte) error {
	m.m[string(key)] = m.m[string(key)] + int(binary.BigEndian.Uint64(value))
	return nil
}

func TestReduce(t *testing.T) {
	input := []struct {
		k     string
		count int
	}{
		{"test1", 1},
		{"test2", 2},
		{"test3", 3},
		{"test4", 4},
		{"horse", 1},
	}

	buf := &bytes.Buffer{}
	enc := gob.NewEncoder(buf)
	for _, in := range input {
		kv := &mapResult{K: []byte(in.k), V: []byte{0, 0, 0, 0, 0, 0, 0, 1}}
		for i := 0; i < in.count; i++ {
			if err := enc.Encode(kv); err != nil {
				t.Fatal(err)
			}
		}
	}

	rw := &reduceWorker{
		reducer: reducerFunc(func(key []byte, values <-chan []byte) []byte {
			var n uint64
			for v := range values {
				n += binary.BigEndian.Uint64(v)
			}
			buf := make([]byte, 8)
			binary.BigEndian.PutUint64(buf, n)
			return buf
		}),
	}

	recordWriter := &mapRecordWriter{
		m: make(map[string]int),
	}

	if err := rw.runReduce(recordWriter, buf); err != nil {
		t.Fatal(err)
	}

	for _, in := range input {
		got := recordWriter.m[in.k]
		if got != in.count {
			t.Errorf("got wrong output for key=%q: got=%d expected=%d", in.k, got, in.count)
		}
	}
}
