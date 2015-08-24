package MapReduce

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"os"
	"path/filepath"
)

type localFileSystem struct{}

func (l *localFileSystem) OpenSectionReader(path string, offset int64, size int64) (io.ReadCloser, error) {
	f, err := os.OpenFile(path, os.O_RDONLY|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}

	type sectionReadCloser struct {
		io.Reader
		io.Closer
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
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}

	return os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
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
		}

		return false
	}

	return true
}

func (l *lineTextRecordReader) Scan() bool {
	if !l.next() {
		return false
	}

	buf := l.s.Bytes()
	k := make([]byte, len(buf))
	copy(k, buf)

	if err := l.s.Err(); err != nil {
		l.err = err
		return false
	}

	l.v = k

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
	path string
	w    io.WriteCloser
	bw   *bufio.Writer
	gw   *gob.Encoder

	partition string
}

type Partitioner interface {
	Partition(key []byte) []byte
	N() int
}

type hashPartitioner struct {
	npartitions uint64
}

func (h *hashPartitioner) N() int {
	return int(h.npartitions)
}

func (h *hashPartitioner) Partition(key []byte) []byte {
	// TODO: use fnv or murmur or something to avoid so many bytes which requires
	// doing big math, we also dont need a crypto hash.
	hash := fnv.New64()
	_, err := hash.Write(key)
	if err != nil {
		panic(err)
	}

	p := hash.Sum64() % h.npartitions
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, p)

	return buf
}

type localOutputCollector struct {
	partitioner Partitioner

	outputs map[string]*outputCollector
}

type mapResult struct {
	K, V []byte
}

func (mr *mapResult) String() string {
	return fmt.Sprintf("key=%q value=%q", mr.K, mr.V)
}

type mapResultList []*mapResult

func (m mapResultList) Less(i, j int) bool {
	switch bytes.Compare(m[i].K, m[j].K) {
	case -1:
		return true
	case 0:
		if bytes.Compare(m[i].V, m[j].V) == -1 {
			return true
		}

		return false
	default:
		return false
	}
}

func (m mapResultList) Swap(i, j int) {
	m[i], m[j] = m[j], m[i]
}

func (m mapResultList) Len() int {
	return len(m)
}

func tempFile(name string) (io.WriteCloser, string, error) {
	// TODO: need to have something which can map the parition(key) -> file so that
	// reduce tasks can read them.
	path := filepath.Join(*tempDir, name)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return nil, "", err
	}

	return f, path, nil
}

func (l *localOutputCollector) Emit(key, value []byte) error {
	pkey := hex.EncodeToString(l.partitioner.Partition(key))
	output, ok := l.outputs[pkey]
	if !ok {
		w, path, err := tempFile(pkey)
		if err != nil {
			// TODO: indicate where this came from?
			return err
		}

		bw := bufio.NewWriter(w)
		output = &outputCollector{
			path:      path,
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

type mapResultResponse struct {
	Files map[string]string
}

// assigns the worker to run over some input data
// this will come via RPC from the master
func (m *mapWorker) Run(path string, offset, size int64) (*mapResultResponse, error) {
	records, err := m.getSplit(path, offset, size)
	if err != nil {
		return nil, err
	}

	// TODO: cleanup on failure
	collector := &localOutputCollector{
		partitioner: m.partitioner,
		outputs:     make(map[string]*outputCollector),
	}

	// TODO: collect stats
	for records.Scan() {
		key, value := records.Pair()

		if err := m.mapper.Map(collector, key, value); err != nil {
			return nil, err
		}
	}

	// TOOD: skip buggy rows?
	if err := records.Err(); err != nil {
		return nil, err
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

	if ferr != nil {
		return nil, ferr
	}

	result := &mapResultResponse{
		Files: make(map[string]string, m.partitioner.N()),
	}

	for _, v := range collector.outputs {
		result.Files[v.partition] = v.path
	}

	return result, nil
}
