package mapreduce

import (
	"flag"
	"fmt"
	"log"
	"os"
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

	if err := os.MkdirAll(*tempDir, 0700); err != nil {
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

	// setup splits
	for _, p := range mr.Input {
		size, err := mr.FileSystem.Size(p)
		if err != nil {
			return err
		}

		if err := mapper.Run(p, 0, size); err != nil {
			return err
		}
	}

	return nil
}
