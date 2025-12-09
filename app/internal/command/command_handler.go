package command

import (
	"errors"
	"fmt"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/internal/keyvaluestore"
	"github.com/codecrafters-io/redis-starter-go/app/internal/respparser"
	"github.com/codecrafters-io/redis-starter-go/app/internal/streamstore"
	"github.com/codecrafters-io/redis-starter-go/app/internal/utils"
)

type CommandHandler interface {
	Process() (respparser.RespContent, error)
}

func (c EchoCommand) Process() (respparser.RespContent, error) {
	resp := respparser.RespContent{
		Value:    c.Message,
		DataType: respparser.BulkString,
	}
	return resp, nil
}

func (c PingCommand) Process() (respparser.RespContent, error) {
	pong := respparser.RespContent{
		Value:    "PONG",
		DataType: respparser.SimpleString,
	}
	return pong, nil
}

func (c GetCommand) Process() (respparser.RespContent, error) {
	get, found := keyvaluestore.Get(c.Key)

	resp := respparser.RespContent{
		Value:    get.Value,
		DataType: respparser.BulkString,
		IsEmpty:  !found,
	}
	return resp, nil
}

func (c SetCommand) Process() (respparser.RespContent, error) {
	keyStoreValue := keyvaluestore.KeyStoreValue{
		InsertedDatetime: time.Now(),
	}

	expires := keyStoreValue.InsertedDatetime.Add(time.Duration(c.RecordExpirationMillis) * time.Millisecond)

	keyStoreValue.Key = c.Key
	keyStoreValue.Value = c.Value
	keyStoreValue.Expire = &expires

	keyvaluestore.Append(keyStoreValue)
	return okResponse, nil
}

func (c TypeCommand) Process() (respparser.RespContent, error) {

	_, foundInKv := keyvaluestore.Get(c.Key)
	if foundInKv {
		resp := respparser.RespContent{
			Value:    "string",
			DataType: respparser.SimpleString,
		}
		return resp, nil
	}

	_, foundInStream := streamstore.GetTopItem(c.Key)
	if foundInStream {
		resp := respparser.RespContent{
			Value:    "stream",
			DataType: respparser.SimpleString,
		}
		return resp, nil
	}

	resp := respparser.RespContent{
		Value:    "none",
		DataType: respparser.SimpleString,
	}
	return resp, nil
}

func (c XaddCommand) Process() (respparser.RespContent, error) {
	streamValue := streamstore.RedisStream{
		StreamKey:               c.StreamKey,
		EntryIdMillisecondsTime: c.EntryIdMillisecondsTime,
		EntryIdSequenceNumber:   c.EntryIdSequenceNumber,
		FieldValues:             c.FieldValues,
		InsertedDatetime:        time.Now(),
	}

	if err := validateEntryId(streamValue); err != nil {
		utils.Log(fmt.Sprintf("(XADD cmd) EntryId validation failed: %e)", err))
		return respparser.RespContent{}, err
	}

	streamstore.Append(streamValue)
	resp := respparser.RespContent{
		Value:    c.GetEntryId(),
		DataType: respparser.BulkString,
	}

	return resp, nil
}

func validateEntryId(s streamstore.RedisStream) error {
	if s.EntryIdMillisecondsTime < 0 || s.EntryIdSequenceNumber <= 0 {
		return errors.New("ERR The ID specified in XADD must be greater than 0-0")
	}

	topItem, found := streamstore.GetTopItem(s.StreamKey)
	if found && (topItem.EntryIdMillisecondsTime > s.EntryIdMillisecondsTime || topItem.EntryIdSequenceNumber >= s.EntryIdSequenceNumber) {
		return errors.New("ERR The ID specified in XADD is equal or smaller than the target stream top item")
	} else {
		return nil
	}
}
