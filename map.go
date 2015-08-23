package mapreduce

type MapOutputCollector interface {
	Emit(key, value []byte) error
}

type Mapper interface {
	Map(output MapOutputCollector, key, value []byte) error
}

type Reducer interface {
	Reduce(key []byte, values <-chan []byte) []byte
}
