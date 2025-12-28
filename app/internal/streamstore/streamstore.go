package streamstore

import (
	"fmt"
	"strconv"
	"time"

	"math"

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
	StreamValues            map[string]string
	InsertedDatetime        time.Time
}

func (s RedisStream) StreamId() string {
	millisStr := strconv.FormatInt(s.EntryIdMillisecondsTime, 10)
	seqNumStr := strconv.Itoa(s.EntryIdSequenceNumber)
	return fmt.Sprintf("%s-%s", millisStr, seqNumStr)
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

func GetItems(streamKey string, startMillis int64, endMillis int64, startSequenceNumber int, endSequenceNumber int) ([]RedisStream, bool) {
	var result []RedisStream
	stream, found := streamStore[streamKey]
	utils.Log(fmt.Sprintf("(StreamStoreValue) GetItems: StreamKey = %s, found = %t", streamKey, found))
	if (!found) || len(stream) < 1 {
		return result, false
	}

	// use default end sequence number when not defined
	if endSequenceNumber == 0 {
		endSequenceNumber = math.MaxInt32
	}

	for _, s := range stream {
		utils.Log(fmt.Sprintf("(StreamStoreValue) Iterating %s", s.StreamId()))

		// Skip entries outside the time range
		if s.EntryIdMillisecondsTime < startMillis || s.EntryIdMillisecondsTime > endMillis {
			continue
		}

		// Check if the entry is within the sequence number range for boundary times
		isStartBoundary := s.EntryIdMillisecondsTime == startMillis &&
			s.EntryIdSequenceNumber >= startSequenceNumber &&
			s.EntryIdSequenceNumber <= endSequenceNumber

		isEndBoundary := s.EntryIdMillisecondsTime == endMillis &&
			s.EntryIdSequenceNumber >= startSequenceNumber &&
			s.EntryIdSequenceNumber <= endSequenceNumber

		isMiddleRange := s.EntryIdMillisecondsTime > startMillis &&
			s.EntryIdMillisecondsTime < endMillis

		if isStartBoundary || isEndBoundary || isMiddleRange {
			result = append(result, s)
		}
	}

	return result, true
}
