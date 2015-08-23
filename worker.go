package mapreduce

import (
	"bufio"
	"crypto/sha512"
	"encoding/gob"
	"encoding/hex"
	"io"
	"log"
	"math/big"
	"os"
	"path/filepath"
)

type localFileSystem struct{}

type sectionReadCloser struct {
	io.Reader
	io.Closer
}

func (l *localFileSystem) OpenSectionReader(path string, offset int64, size int64) (io.ReadCloser, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	return &sectionReadCloser{
		Reader: bufio.NewReader(io.NewSectionReader(f, offset, size)),
		Closer: f,
	}, nil
}

func (l *localFileSystem) Size(path string) (int64, error) {
	stat, err := os.Stat(path)
	if err != nil {
		return 0, err
	}

	return stat.Size(), nil
}

func (l *localFileSystem) Open(path string) (io.ReadWriteCloser, error) {
	return os.Open(path)
}

// this is for reading, can we unify one for writing?
type FileSystem interface {
	OpenSectionReader(path string, offset, size int64) (io.ReadCloser, error)
	Size(path string) (int64, error)
	Open(path string) (io.ReadWriteCloser, error)
}

type RecordScanner interface {
	Scan() bool
	Pair() (key, value []byte)
	Err() error
}

type InputFormat interface {
	Records(r io.Reader) (RecordScanner, error)
}

type TextInputFormat struct{}

func (*TextInputFormat) Records(r io.Reader) (RecordScanner, error) {
	return &lineTextRecordReader{
		s: bufio.NewScanner(r),
	}, nil
}

type lineTextRecordReader struct {
	s    *bufio.Scanner
	k, v []byte // next
	err  error
}

func (l *lineTextRecordReader) Pair() ([]byte, []byte) {
	return l.k, l.v
}

func (l *lineTextRecordReader) next() bool {
	// reads a single KV pair
	if !l.s.Scan() {
		if err := l.s.Err(); err != nil {
			l.err = err
			return false
		}

		return false
	}

	return true
}

func (l *lineTextRecordReader) Scan() bool {
	if !l.next() {
		return false
	}

	// TOOD: reuse l.k and l.v if there is cap
	buf := l.s.Bytes()
	k := make([]byte, len(buf))
	copy(k, buf)

	if !l.next() {
		l.err = io.EOF
		return false
	}

	buf = l.s.Bytes()
	v := make([]byte, len(buf))
	copy(v, buf)

	if err := l.s.Err(); err != nil {
		l.err = err
		return false
	}

	l.k = k
	l.v = v

	return true
}

func (l *lineTextRecordReader) Err() error {
	return l.err
}

type mapWorker struct {
	mapper Mapper

	partitioner Partitioner
	fs          FileSystem

	inputFormat InputFormat
}

type outputCollector struct {
	w  io.WriteCloser
	bw *bufio.Writer
	gw *gob.Encoder

	partition string
}

type Partitioner interface {
	Partition(key []byte) []byte
}

type hashPartitioner struct {
	npartitions int64
}

func (h *hashPartitioner) Partition(key []byte) []byte {
	hash := sha512.Sum512(key)

	bint := big.NewInt(0)
	bint = bint.SetBytes(hash[:])
	bint = bint.Mod(bint, big.NewInt(h.npartitions))

	if bint.BitLen() == 0 {
		return []byte{0}
	}

	return bint.Bytes()
}

type localOutputCollector struct {
	partitioner Partitioner

	outputs map[string]*outputCollector
}

type mapResult struct {
	K, V []byte
}

func tempFile(name string) (io.WriteCloser, error) {
	// TODO: need to have something which can map the parition(key) -> file so that
	// reduce tasks can read them.
	path := filepath.Join(*tempDir, name)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}

	return f, nil
}

func (l *localOutputCollector) Emit(key, value []byte) error {
	pkey := hex.EncodeToString(l.partitioner.Partition(key))
	output, ok := l.outputs[pkey]
	if !ok {
		w, err := tempFile(pkey)
		if err != nil {
			// TODO: indicate where this came from?
			return err
		}

		bw := bufio.NewWriter(w)
		output = &outputCollector{
			w:         w,
			bw:        bw,
			gw:        gob.NewEncoder(bw),
			partition: pkey,
		}

		l.outputs[pkey] = output
	}

	// how to store the intermediate data? For now just GOB
	if err := output.gw.Encode(mapResult{key, value}); err != nil {
		return err
	}

	return nil
}

func (m *mapWorker) getSplit(path string, offset, size int64) (RecordScanner, error) {
	r, err := m.fs.OpenSectionReader(path, offset, size)
	if err != nil {
		return nil, err
	}

	split, err := m.inputFormat.Records(r)
	if err != nil {
		return nil, err
	}

	return split, nil
}

// assigns the worker to run over some input data
// this will come via RPC from the master
func (m *mapWorker) Run(path string, offset, size int64) error {
	records, err := m.getSplit(path, offset, size)
	if err != nil {
		return err
	}

	// TODO: cleanup on failure
	collector := &localOutputCollector{
		partitioner: m.partitioner,
		outputs:     make(map[string]*outputCollector),
	}

	// TODO: collect stats
	for records.Scan() {
		key, value := records.Pair()
		if err != nil {
			if err == io.EOF {
				// end of input
				break
			}
		}

		if err := m.mapper.Map(collector, key, value); err != nil {
			return err
		}
	}

	// TOOD: skip buggy rows?
	if err := records.Err(); err != nil {
		return err
	}

	// final error
	var ferr error
	// flush all the buffers, we are done
	for _, out := range collector.outputs {
		// how to handle a single file failed? Just retry the whole split
		if err := out.bw.Flush(); err != nil {
			log.Printf("unable to flush map output for partition %v: %v", out.partition, err)
			if ferr != nil {
				ferr = err
			}
		}

		if err := out.w.Close(); err != nil {
			log.Printf("unable to close map output for partition %v: %v", out.partition, err)
			// we could probably just bail out early, best not to leak FDs but the
			// child process should be killed
			if ferr != nil {
				ferr = err
			}
		}
	}

	return ferr
}
