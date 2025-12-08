package command

import (
	"github.com/codecrafters-io/redis-starter-go/app/internal/keyvaluestore"
	"github.com/codecrafters-io/redis-starter-go/app/internal/respparser"
)

type Command struct {
	CommandType   string
	CommandValues []string
}

type CommandContext struct {
	KeyStore *keyvaluestore.KeyStore
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
	Message string
}

type GetCommand struct {
	Key string
}

type SetCommand struct {
	Key                    string
	Value                  string
	RecordExpirationMillis int
}

type TypeCommand struct {
	Key string
}

type XaddCommand struct {
	StreamKey   string
	EntryId     string
	FieldValues map[string]string
}

func ErrorResponse(e error) CommandResponse {
	errContent := respparser.RespContent{
		Value:    e.Error(),
		DataType: respparser.SimpleError,
	}
	return CommandResponse{Value: errContent}
}
