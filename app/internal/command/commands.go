package command

import (
	"github.com/codecrafters-io/redis-starter-go/app/internal/respparser"
	"strings"
)

type Command struct {
	CommandType   string
	CommandValues []string
}

type CommandResponse struct {
	Value []byte
}

func handleEcho(command *Command) CommandResponse {
	echoResponse := strings.Join(command.CommandValues, " ")
	resp := respparser.RespContent{
		Value:    echoResponse,
		DataType: respparser.BulkString,
	}
	encoded := respparser.EncodeBulkString(resp)
	return CommandResponse{encoded}
}

func handlePing() CommandResponse {
	response := "+PONG\r\n" // TODO encode simple string!
	return CommandResponse{[]byte(response)}
}

// func handleSet(command *Command) CommandResponse {
// 	return
// }

func ParseCommand(cmd []byte) (*Command, error) {
	// A client sends the Redis server an array consisting of only bulk strings.
	// command example *2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n
	arrayElements, error := respparser.ParseArray(cmd)
	if error != nil {
		// TODO error
		return nil, error
	}

	command := elementsToCommand(arrayElements)
	return &command, nil
}

func ProcessCommand(command *Command) (CommandResponse, error) {
	switch strings.ToUpper(command.CommandType) {
	case "PING":
		pingResponse := handlePing()
		return pingResponse, nil
	case "ECHO":
		echoResponse := handleEcho(command)
		return echoResponse, nil
	default:
		// ping for now
		pingResponse := handlePing()
		return pingResponse, nil
	}
}

func elementsToCommand(elements []respparser.RespContent) Command {
	if len(elements) == 0 {
		// TODO return error instead PING
		return Command{
			CommandType:   "PING",
			CommandValues: nil,
		}
	} else if len(elements) == 1 {
		return Command{
			CommandType:   elements[0].Value,
			CommandValues: nil,
		}
	} else {
		commandType := elements[0].Value
		commandValues := make([]string, 0)
		for _, content := range elements[1:] {
			commandValues = append(commandValues, content.Value)
		}
		return Command{
			CommandType:   commandType,
			CommandValues: commandValues,
		}
	}
}
