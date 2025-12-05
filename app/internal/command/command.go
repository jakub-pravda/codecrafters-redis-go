package command

import (
	"github.com/codecrafters-io/redis-starter-go/app/internal/respparser"
	"github.com/codecrafters-io/redis-starter-go/app/internal/store"
)

type Command struct {
	CommandType   string
	CommandValues []string
}

type CommandContext struct {
	KeyStore *store.KeyStore
}

type CommandResponse struct {
	Value respparser.RespContent
}

var okResponse = respparser.RespContent{
	Value:    "OK",
	DataType: respparser.SimpleString,
}

type PingCommand struct{}

type EchoCommand struct {
	message string
}

type GetCommand struct {
	key string
}

type SetCommand struct {
	key                    string
	value                  string
	recordExpirationMillis int
}

type TypeCommand struct {
	key string
}

func ErrorResponse(e error) CommandResponse {
	errContent := respparser.RespContent{
		Value:    e.Error(),
		DataType: respparser.SimpleError,
	}
	return CommandResponse{Value: errContent}
}
