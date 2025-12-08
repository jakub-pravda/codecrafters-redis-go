package command

import (
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/internal/keyvaluestore"
	"github.com/codecrafters-io/redis-starter-go/app/internal/respparser"
	"github.com/codecrafters-io/redis-starter-go/app/internal/streamstore"
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

	_, foundInStream := streamstore.Get(c.Key)
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
		StreamKey:        c.StreamKey,
		EntryId:          c.EntryId,
		FieldValues:      c.FieldValues,
		InsertedDatetime: time.Now(),
	}
	streamstore.Append(streamValue)
	resp := respparser.RespContent{
		Value:    streamValue.EntryId,
		DataType: respparser.BulkString,
	}

	return resp, nil
}
