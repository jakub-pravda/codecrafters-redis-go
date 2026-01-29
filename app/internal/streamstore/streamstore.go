package streamstore

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"math"

	"github.com/codecrafters-io/redis-starter-go/app/internal/respparser"
	"github.com/codecrafters-io/redis-starter-go/app/internal/utils"
)

type StreamStore struct {
	mu    sync.RWMutex
	store map[string][]RedisStream
}

var streamStoreChannel chan RedisStream
var notificationChannel chan RedisStream
var streamStore StreamStore

func InitStreamStore() {
	streamStoreChannel = make(chan RedisStream)
	streamStore = StreamStore{
		store: map[string][]RedisStream{},
	}
}

func StreamStoreListener() {
	defer close(streamStoreChannel)

	utils.Log("(StreamStoreListener) Starting listener")
	if streamStoreChannel != nil {
		for {
			value, chanOk := <-streamStoreChannel

			select {
			case notificationChannel <- value:
			default:
				// no listeners, skip notification
			}

			if !chanOk {
				utils.Log("(StreamStoreListener) Channel closed")
				break
			}

			streamStore.mu.Lock()
			stream, found := streamStore.store[value.StreamKey]

			if found {
				// update existing strem
				utils.Log(fmt.Sprintf("(StreamStoreListener) Append: StreamKey = %s - appending to an existing stream", value.StreamKey))
				appendStream := append(stream, value)
				streamStore.store[value.StreamKey] = appendStream
			} else {
				// create new stream
				utils.Log(fmt.Sprintf("(StreamStoreListener) Append: StreamKey = %s - creating a new stream", value.StreamKey))
				streamStore.store[value.StreamKey] = []RedisStream{value}
			}
			streamStore.mu.Unlock()
		}
	}
	utils.Log("(StreamStoreListener) Closing listener")
}

func GetStreamStoreChannel() chan<- RedisStream {
	return streamStoreChannel
}

func GetStreamNotificationChannel() <-chan RedisStream {
	// TODO refactor mutex?
	if notificationChannel == nil {
		notificationChannel = make(chan RedisStream)
	}
	return notificationChannel
}

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

func (s RedisStream) ToRespArray() respparser.Array {
	// create key - value arrays
	keyValues := []respparser.RespData{}
	for k, v := range s.StreamValues {
		keyValue := respparser.Array{
			Items: []respparser.RespData{
				respparser.BulkString{
					Value: k,
				},
				respparser.BulkString{
					Value: v,
				},
			},
		}
		keyValues = append(keyValues, keyValue)
	}

	streamIdArray := respparser.Array{
		Items: append([]respparser.RespData{
			respparser.BulkString{
				Value: s.StreamId(),
			},
		}, keyValues...),
	}
	return streamIdArray
}

func (s RedisStream) StreamId() string {
	millisStr := strconv.FormatInt(s.EntryIdMillisecondsTime, 10)
	seqNumStr := strconv.Itoa(s.EntryIdSequenceNumber)
	return fmt.Sprintf("%s-%s", millisStr, seqNumStr)
}

func GetTopItem(streamKey string) (RedisStream, bool) {
	streamStore.mu.RLock()
	defer streamStore.mu.RUnlock()

	stream, found := streamStore.store[streamKey]
	utils.Log(fmt.Sprintf("(StreamStoreValue) Get: StreamKey = %s, found = %t", streamKey, found))
	if (!found) || len(stream) < 1 {
		return RedisStream{}, false
	}
	last := stream[len(stream)-1]
	return last, true
}

func GetItems(streamKey string, startMillis int64, endMillis int64, startSequenceNumber int, endSequenceNumber int) ([]RedisStream, bool) {
	streamStore.mu.RLock()
	defer streamStore.mu.RUnlock()

	var result []RedisStream
	stream, found := streamStore.store[streamKey]
	utils.Log(fmt.Sprintf("(StreamStoreValue) GetItems: StreamKey = %s, found = %t", streamKey, found))
	if (!found) || len(stream) < 1 {
		return result, false
	}

	// use default end sequence number when not defined
	if endSequenceNumber == 0 {
		endSequenceNumber = math.MaxInt32
	}

	for _, s := range stream {
		utils.Log(fmt.Sprintf("(StreamStoreValue)(GetItems) Iterating %s", s.StreamId()))

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

func GetItemsByFilter(streamKey string, filter func(i []RedisStream) []RedisStream) ([]RedisStream, bool) {
	streamStore.mu.RLock()
	defer streamStore.mu.RUnlock()

	var result []RedisStream
	stream, found := streamStore.store[streamKey]
	utils.Log(fmt.Sprintf("(StreamStoreValue)(GetItemsByFilter) GetItems: StreamKey = %s, found = %t", streamKey, found))
	if (!found) || len(stream) < 1 {
		return result, false
	}

	filteredStream := filter(stream)
	if len(filteredStream) > 0 {
		return filteredStream, true
	} else {
		return nil, false
	}
}

func GetItemsByFilterChan(streamKey string, filter func(i []RedisStream) []RedisStream, ctx context.Context) <-chan []RedisStream {
	resultChannel := make(chan []RedisStream, 1)
	utils.Log(fmt.Sprintf("(StreamStoreValue)(GetItemsByFilterChan) GetItems: StreamKey = %s", streamKey))

	go func() {
		defer close(resultChannel)

		for {
			select {
			case <-ctx.Done():
				utils.Log(fmt.Sprintf("(StreamStoreValue)(GetItemsByFilterChan) GetItems: StreamKey = %s, timeout", streamKey))
				return
			default:
				result, found := GetItemsByFilter(streamKey, filter)
				if found {
					utils.Log(fmt.Sprintf("(StreamStoreValue)(GetItemsByFilterChan) GetItems: StreamKey = %s, result size: %d", streamKey, len(result)))
					resultChannel <- result
					return
				}
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()
	return resultChannel
}
