package command

import (
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/internal/respparser"
	"github.com/codecrafters-io/redis-starter-go/app/internal/store"
)

type CommandHandler interface {
	Process() (respparser.RespContent, error)
}

func (c EchoCommand) Process() (respparser.RespContent, error) {
	resp := respparser.RespContent{
		Value:    c.message,
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
	get := store.Get(c.key)
	isEmpty := get == store.KeyStoreValue{}

	resp := respparser.RespContent{
		Value:    get.Value,
		DataType: respparser.BulkString,
		IsEmpty:  isEmpty,
	}
	return resp, nil
}

func (c SetCommand) Process() (respparser.RespContent, error) {
	keyStoreValue := store.KeyStoreValue{
		InsertedDatetime: time.Now(),
	}

	expires := keyStoreValue.InsertedDatetime.Add(time.Duration(c.recordExpirationMillis) * time.Millisecond)

	keyStoreValue.Key = c.key
	keyStoreValue.Value = c.value
	keyStoreValue.Expire = &expires

	store.Append(keyStoreValue)
	return okResponse, nil
}

func (c TypeCommand) Process() (respparser.RespContent, error) {
	get := store.Get(c.key)
	isEmpty := get == store.KeyStoreValue{}

	getType := "none"
	if !isEmpty {
		// TODO support other types
		getType = "string"
	}

	resp := respparser.RespContent{
		Value:    getType,
		DataType: respparser.SimpleString,
	}
	return resp, nil
}
