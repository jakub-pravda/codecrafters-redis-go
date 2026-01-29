package command

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/internal/respparser"
	"github.com/codecrafters-io/redis-starter-go/app/internal/streamstore"
	"github.com/codecrafters-io/redis-starter-go/app/internal/utils"
)

type XReadCommand struct {
	Streams     []XReadStream
	BlockMillis int
	IsBlocking  bool
}

func (c XReadCommand) Process() (respparser.RespData, error) {
	utils.Log("(XReadCommand) Processing XRead command")
	streams := []respparser.RespData{}

	for _, stream := range c.Streams {

		entryId := stream.entryId
		streamKey := stream.streamKey
		utils.Log(fmt.Sprintf("(XReadCommand) JPR STREAM %s", streamKey))

		// Create stream filter for this use case
		streamFilter := func(stream streamstore.RedisStream) bool {
			if stream.EntryIdMillisecondsTime == entryId.MillisecondsTime && stream.EntryIdSequenceNumber == entryId.SequenceNumber {
				// exclusive search
				return false
			}

			return stream.EntryIdMillisecondsTime >= entryId.MillisecondsTime && stream.EntryIdSequenceNumber >= entryId.SequenceNumber
		}

		streamFilterArray := func(filter []streamstore.RedisStream) []streamstore.RedisStream {
			// XREAD is exclusive - command retrieves records with ID greater than the specified ID
			filtered := []streamstore.RedisStream{}

			for _, e := range filter {
				if streamFilter(e) {
					filtered = append(filtered, e)
				}
			}

			return filtered
		}

		var result []streamstore.RedisStream = nil
		var found bool = false

		if !entryId.StreamTopItems {
			result, found = streamstore.GetItemsByFilter(streamKey, streamFilterArray)
		}

		if !found && c.IsBlocking {
			var ctx context.Context
			var cancel context.CancelFunc
			if c.BlockMillis <= 0 {
				// blocking read indefinitely
				utils.Log(fmt.Sprintf("(XReadCommand) Stream with key %s not found, waiting for response", streamKey))
				ctx, cancel = context.WithCancel(context.Background())
			} else {
				// blocking reads with timeout
				utils.Log(fmt.Sprintf("(XReadCommand) Stream with key %s not found, waiting for %d milliseconds for response", streamKey, c.BlockMillis))
				ctx, cancel = context.WithTimeout(context.Background(), time.Duration(c.BlockMillis)*time.Millisecond)
			}
			defer cancel()

			streamNotificationChannel := streamstore.GetStreamNotificationChannel()

			for !found {
				select {
				case s := <-streamNotificationChannel:
					if entryId.StreamTopItems || streamFilter(s) {
						result = []streamstore.RedisStream{
							s,
						}
						found = true
					}
				case <-ctx.Done():
					// remark: when timeout occurs, nil array is returned
					nilArray := respparser.Array{IsNull: true}
					return nilArray, nil
				}
			}
		} else if !found {
			utils.Log(fmt.Sprintf("(XReadCommand) Stream with key %s not found", streamKey))
			continue
		}

		streamData := make([]respparser.RespData, 0)
		for _, redisStream := range result {
			streamData = append(streamData, redisStream.ToRespArray())
		}

		stream := respparser.Array{
			Items: []respparser.RespData{
				respparser.BulkString{Value: streamKey},
				respparser.Array{Items: streamData},
			},
		}
		utils.Log(fmt.Sprintf("(XReadCommand) Appending stream with stream key %s to result", streamKey))
		streams = append(streams, stream)
	}

	resultArray := respparser.Array{
		Items: streams,
	}

	utils.Log(fmt.Sprintf("(XReadCommand) Return %v", resultArray))
	return resultArray, nil
}

func parseXReadCommand(command *Command) (XReadCommand, error) {
	if command.CommandType != "XREAD" {
		return XReadCommand{}, errors.New("Not a XREAD")
	}

	xReadCommand := XReadCommand{}

	for i, value := range command.CommandValues {
		upperValue := strings.ToUpper(value)

		switch upperValue {
		case "BLOCK":
			if i+1 >= len(command.CommandValues) {
				return xReadCommand, fmt.Errorf("missing BLOCK value")
			}

			blockMillis, err := strconv.Atoi(command.CommandValues[i+1])
			if err != nil {
				utils.Log(fmt.Sprintf("ERROR (parseXReadCommand) Invalid BLOCK value. Int expected, but got: %s. Error: %v", command.CommandValues[i+1], err))
				return xReadCommand, err
			}

			xReadCommand.BlockMillis = blockMillis
			xReadCommand.IsBlocking = true

		case "STREAMS":
			streams := command.CommandValues[i+1:]
			if len(streams)%2 != 0 {
				err := fmt.Errorf("ERROR (parseXReadCommand) Even number of streams value expected, but got: %d", len(streams))
				utils.Log(err.Error())
				return xReadCommand, err
			}

			for j := 0; j < len(streams)/2; j++ {
				key := streams[j]
				id := streams[len(streams)/2+j]

				// handle special ids
				if id == "$" {
					// stream new received items only
					entryId := EntryId{StreamTopItems: true}
					readStream := XReadStream{
						entryId:   entryId,
						streamKey: key,
					}
					xReadCommand.Streams = append(xReadCommand.Streams, readStream)
				} else {
					// parse entry id
					entryId, err := parseXCommandsEntryId(id)
					if err != nil {
						utils.Log(fmt.Sprintf("ERROR (parseXReadCommand) Can't parse stream entry id: %s, %v", id, err))
						return xReadCommand, err
					}
					readStream := XReadStream{
						entryId:   entryId,
						streamKey: key,
					}

					utils.Log(fmt.Sprintf("(parseXReadCommand) Stream key: %s, id: %s", key, id))
					xReadCommand.Streams = append(xReadCommand.Streams, readStream)
				}
			}
		}
	}

	return xReadCommand, nil
}
