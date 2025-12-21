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
	StreamKey               string
	EntryIdMillisecondsTime int64
	EntryIdSequenceNumber   int
	FieldValues             map[string]string
	InsertedDatetime        time.Time
}

func Append(value RedisStream) {
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

func GetTopItem(streamKey string) (RedisStream, bool) {
	// TODO get stream implementation
	stream, found := streamStore[streamKey]
	utils.Log(fmt.Sprintf("(StreamStoreValue) Get: StreamKey = %s, found = %t", streamKey, found))
	if (!found) || len(stream) < 1 {
		return RedisStream{}, false
	}
	last := stream[len(stream)-1]
	return last, true
}
