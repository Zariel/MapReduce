package mapreduce

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
)

type reduceWorker struct {
	reducer Reducer

	workerID     int
	outputPath   string
	outputFormat OutputFormat

	fs        FileSystem
	partition string
	fileCount uint64

	mu    sync.Mutex
	paths []string
}

func (rw *reduceWorker) loadData(path string) error {
	// This is the shuffle phase
	// load data and sort

	// TODO: this FS wont hold the intermediate map output, this needs to be fetched
	// via RPC
	r, err := rw.fs.Open(path)
	if err != nil {
		return err
	}
	defer r.Close()

	br := bufio.NewReader(r)

	// TODO: need to implement external merge sort
	data := make([]*mapResult, 0)
	dec := gob.NewDecoder(br)

	for {
		// TODO: need to use a format which doesnt require expensive parsing as
		// it will go from map -> disk -> network -> disk -> sort -> disk
		res := &mapResult{}
		if err := dec.Decode(res); err != nil {
			if err == io.EOF {
				break
			}

			return err
		}

		data = append(data, res)
	}

	sort.Sort(mapResultList(data))
	n := atomic.AddUint64(&rw.fileCount, 1) - 1
	// TODO: need shorter file names
	outpath := filepath.Join(*tempDir, fmt.Sprintf("/reduce-%s/part-%d", rw.partition, n))

	if err := os.MkdirAll(filepath.Dir(outpath), 0700); err != nil {
		return err
	}

	w, err := os.OpenFile(outpath, os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer w.Close()

	bw := bufio.NewWriter(w)
	enc := gob.NewEncoder(bw)
	for _, kv := range data {
		if err := enc.Encode(kv); err != nil {
			return err
		}
	}

	if err := bw.Flush(); err != nil {
		return err
	}

	rw.mu.Lock()
	rw.paths = append(rw.paths, outpath)
	rw.mu.Unlock()

	return nil
}

type lazyReader struct {
	io.Closer

	dec *gob.Decoder
	kv  *mapResult
}

func (r *lazyReader) next() (bool, error) {
	r.kv = nil
	// do we need to reallocate next each iteration?
	kv := &mapResult{}
	if err := r.dec.Decode(kv); err != nil {
		if err == io.EOF {
			return false, nil
		}

		return false, err
	}

	r.kv = kv

	return true, nil
}

type lazyReaderHeap []*lazyReader

func (lrh lazyReaderHeap) Len() int {
	return len(lrh)
}

func (lrh lazyReaderHeap) Less(i, j int) bool {
	if bytes.Compare(lrh[i].kv.K, lrh[j].kv.K) <= 0 {
		return true
	} else {
		return bytes.Compare(lrh[i].kv.V, lrh[j].kv.V) == -1
	}
}

func (lrh *lazyReaderHeap) Push(x interface{}) {
	*lrh = append(*lrh, x.(*lazyReader))
}

func (lrh *lazyReaderHeap) Pop() interface{} {
	old := *lrh
	n := len(old)
	x := old[n-1]
	*lrh = old[:n-1]
	return x
}

func (rw *reduceWorker) sortAndLoad() (io.ReadCloser, error) {
	// TODO: simplify this, it can leak open files which is bad and does too much
	outpath := filepath.Join(*tempDir, fmt.Sprintf("/reduce-%s/sorted", rw.partition))

	f, err := os.OpenFile(outpath, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	var h lazyReaderHeap
	for _, path := range rw.paths {
		r, err := os.Open(path)
		if err != nil {
			// will previously opened files FD leak?
			return nil, err
		}

		lr := &lazyReader{
			Closer: r,
			dec:    gob.NewDecoder(bufio.NewReader(r)),
		}

		if _, err := lr.next(); err != nil {
			return nil, err
		}

		h.Push(lr)
	}

	bw := bufio.NewWriter(f)
	enc := gob.NewEncoder(bw)

	for h.Len() > 0 {
		lr := h.Pop().(*lazyReader)
		if lr.kv == nil {
			// TODO: remove this should never happen
			panic("reader had nil kv")
		}

		if err := enc.Encode(lr.kv); err != nil {
			return nil, err
		}

		more, err := lr.next()
		if err != nil {
			return nil, err
		} else if more {
			h.Push(lr)
		} else {
			lr.Close()
		}
	}

	if err := bw.Flush(); err != nil {
		log.Println(err)
		return nil, err
	}

	if _, err := f.Seek(0, 0); err != nil {
		return nil, err
	}

	return f, nil
}

func (rw *reduceWorker) Run() (err error) {
	f, err := rw.sortAndLoad()
	if err != nil {
		return err
	}
	defer f.Close()

	output, err := rw.fs.Open(rw.outputPath)
	if err != nil {
		return err
	}
	defer func() {
		cerr := output.Close()
		if cerr != nil && err == nil {
			err = cerr
		}
	}()

	// TODO: have this controlled by the FileSystem
	bw := bufio.NewWriter(output)

	if err = rw.runReduce(output, f); err != nil {
		return
	}

	if err = bw.Flush(); err != nil {
		return
	}

	return nil
}

func (rw *reduceWorker) runReduce(output io.Writer, input io.Reader) error {

	last := &mapResult{}
	kv := &mapResult{}

	recordWriter := rw.outputFormat.RecordWriter(output)

	dec := gob.NewDecoder(bufio.NewReader(input))
	for {
		err := dec.Decode(kv)
		if err != nil {
			if err == io.EOF {
				break
			}

			return err
		}

		// TODO: do we need to copy this?
		key := kv.K

		ch := make(chan []byte, 2)
		ch <- kv.V
		go func() {
			defer close(ch)
			// TODO: how to handle errors from iterating here?

			*last = *kv

			for bytes.Equal(last.K, kv.K) {
				err := dec.Decode(last)
				if err != nil {
					if err == io.EOF {
						break
					}

					log.Println(err)
					return
				}

				ch <- last.V
			}

			*kv = *last
		}()

		res := rw.reducer.Reduce(key, ch)

		if err := recordWriter.WriteRecord(key, res); err != nil {
			return err
		}
	}

	return nil
}

type RecordWriter interface {
	WriteRecord(key, value []byte) error
}

type OutputFormat interface {
	RecordWriter(w io.Writer) RecordWriter
}
