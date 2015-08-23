package mapreduce

type MapOutputCollector interface {
	Emit(key, value []byte) error
}

type Mapper interface {
	Map(output MapOutputCollector, key, value []byte) error
}

type ReduceOutputCollector interface {
	Emit(value []byte)
}

type Reducer interface {
	Reduce(output ReduceOutputCollector, key []byte, values <-chan []byte) error
}
