package mapreduce

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

var mesosDir string

func init() {
	var ok bool
	mesosDir, ok = os.LookupEnv("MESOS_DIRECTORY")
	if !ok {
		mesosDir = os.TempDir()
	}

}

var (
	binpath    = flag.String("mapreduce.bin", "", "the `file` to execute to run MapReduce tasks")
	master     = flag.String("mapreduce.master", "", "the `address` of the MapReduce master task")
	addr       = flag.String("mapreduce.addr", "", "local `address` to listen on")
	taskMode   = flag.String("mapreduce.task_mode", "", "task mode to run as, master, map or reduce")
	standalone = flag.Bool("mapreduce.standalone", false, "run in standalone on a single machine not in mesos")
	tempDir    = flag.String("mapreduce.tempdir", mesosDir, "the `directory` to store intermediate data in")
)

func New(mapper Mapper, reducer Reducer, input ...string) *MapReduce {
	return &MapReduce{
		Mapper:  mapper,
		Reducer: reducer,
		Input:   input,
	}
}

type MapReduce struct {
	Mapper  Mapper
	Reducer Reducer
	Input   []string

	Partitioner    Partitioner
	InputBlockSize int64
	FileSystem     FileSystem
}

func (mr *MapReduce) Run() error {
	flag.Parse()

	if mr.FileSystem == nil {
		mr.FileSystem = &localFileSystem{}
	}
	if mr.Partitioner == nil {
		mr.Partitioner = &hashPartitioner{4}
	}

	err := os.MkdirAll(*tempDir, 0700)
	if err != nil {
		return err
	}

	*tempDir, err = filepath.Abs(*tempDir)
	if err != nil {
		return err
	}

	if *master == "" {
		// no master, we are master start up
		if err := mr.startMaster(*addr); err != nil {
			return fmt.Errorf("unable to start master: %v", err)
		}
	} else {
		switch *taskMode {
		case "map":
		case "reduce":
		default:
			panic(fmt.Sprintf("unknown task mode: %q", *taskMode))
		}
	}

	return nil
}

func (mr *MapReduce) startMaster(addr string) error {
	log.Printf("starting master on %v\n", addr)

	// create a single mapper and give it the splits
	mapper := mapWorker{
		mapper:      mr.Mapper,
		partitioner: mr.Partitioner,
		fs:          mr.FileSystem,
		inputFormat: &TextInputFormat{}, // TODO pass this in
	}

	partitions := make(map[string][]string)

	// setup splits
	for _, p := range mr.Input {
		size, err := mr.FileSystem.Size(p)
		if err != nil {
			return err
		}

		// TODO: retry
		result, err := mapper.Run(p, 0, size)
		if err != nil {
			return err
		}

		for partition, path := range result.Files {
			partitions[partition] = append(partitions[partition], path)
		}
	}

	// We will have R partitions at this point, we can lazilly load and sort after
	// each partition part is finished by a map task (shuffle phase) when loading
	// onto the reducers local disks.
	// For now we just need 1 reducer per partion to run
	for partition, paths := range partitions {
		reducer := &reduceWorker{
			fs:        mr.FileSystem,
			partition: partition,
		}

		for _, path := range paths {
			if err := reducer.loadData(path); err != nil {
				// THis error could indicate that the mapper failed, that the data
				// could not fit into memory or various other things.
				// TODO: Properly handle all cases for this error
				return err
			}
		}

		if err := reducer.Run(); err != nil {
			return fmt.Errorf("reduce failed for partition %q: %v\n", partition, err)
		}
	}

	return nil
}
