package streamstore

import (
	"fmt"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/internal/utils"
)

type StreamStore map[string][]RedisStream

var streamStore = make(StreamStore)

// streamKey ->
//   Entry1 (key value)
//   Entry2 (key value)
//   Entry3 (key value)

type RedisStream struct {
	StreamKey        string
	EntryId          string
	FieldValues      map[string]string
	InsertedDatetime time.Time
}

func Append(value RedisStream) {
	utils.Log(fmt.Sprintf("(StreamStoreValue) Append: StreamKey = %s, EntryId = %s", value.StreamKey, value.EntryId))

	stream, found := streamStore[value.StreamKey]

	if found {
		// update existing strem
		utils.Log(fmt.Sprintf("(StreamStoreValue) Append: StreamKey = %s - appending to an existing stream", value.StreamKey))
		appendStream := append(stream, value)
		streamStore[value.StreamKey] = appendStream
	} else {
		// create new stream
		utils.Log(fmt.Sprintf("(StreamStoreValue) Append: StreamKey = %s - creating a new stream", value.StreamKey))
		streamStore[value.StreamKey] = []RedisStream{value}
	}
}

func Get(streamKey string) (RedisStream, bool) {
	// TODO get stream implementation
	_, found := streamStore[streamKey]
	utils.Log(fmt.Sprintf("(StreamStoreValue) Get: StreamKey = %s, found = %t", streamKey, found))
	return RedisStream{}, found
}
