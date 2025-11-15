package command

import (
	"errors"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/internal/respparser"
	"github.com/codecrafters-io/redis-starter-go/app/internal/store"
)

type Command struct {
	CommandType   string
	CommandValues []string
}

// TODO context is bad naming
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

func handleEcho(command *Command) (respparser.RespContent, error) {
	if command.CommandType != "ECHO" {
		return respparser.RespContent{}, errors.New("Not an ECHO")
	}

	echoResponse := strings.Join(command.CommandValues, " ")
	resp := respparser.RespContent{
		Value:    echoResponse,
		DataType: respparser.BulkString,
	}
	return resp, nil
}

func handleGet(command *Command) (respparser.RespContent, error) {
	if command.CommandType != "GET" {
		return respparser.RespContent{}, errors.New("Not a GET")
	} else if len(command.CommandValues) != 1 {
		return respparser.RespContent{}, errors.New("Get must contains only key asr command arg")
	}

	get := store.Get(command.CommandValues[0])

	isEmpty := get == store.StorageValue{}

	resp := respparser.RespContent{
		Value:    get.Value,
		DataType: respparser.BulkString,
		IsEmpty:  isEmpty,
	}
	return resp, nil
}

func handleSet(command *Command) (respparser.RespContent, error) {
	var key string
	var value string
	for n, arg := range command.CommandValues {
		if n == 0 {
			key = arg
			continue
		} else if n == 1 {
			value = arg
			continue
		} else {
			// handle other args
			continue
		}
	}

	if key == "" {
		err := errors.New("SET Key missing in command args")
		return respparser.RespContent{}, err
	}

	if value == "" {
		err := errors.New("SET value missing in command args")
		return respparser.RespContent{}, err
	}

	keyStoreValue := store.StorageValue{
		Key:              key,
		Value:            value,
		InsertedDatetime: time.Now(),
	}
	store.Append(keyStoreValue)
	return okResponse, nil
}

func handlePing() respparser.RespContent {
	pong := respparser.RespContent{
		Value:    "PONG",
		DataType: respparser.SimpleString,
	}
	return pong
}

func ParseCommand(cmd []byte) (*Command, error) {
	// A client sends the Redis server an array consisting of only bulk strings.
	// command example *2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n
	arrayElements, error := respparser.ParseArray(cmd)
	if error != nil {
		return nil, error
	}

	command := elementsToCommand(arrayElements)
	return &command, nil
}

func ProcessCommand(command *Command) (CommandResponse, error) {
	switch strings.ToUpper(command.CommandType) {
	case "PING":
		pingResponse := handlePing()
		return CommandResponse{pingResponse}, nil
	case "ECHO":
		echoResponse, err := handleEcho(command)
		if err != nil {
			return CommandResponse{}, err
		}
		return CommandResponse{echoResponse}, nil
	case "GET":
		getResponse, err := handleGet(command)
		if err != nil {
			return CommandResponse{}, err
		}
		return CommandResponse{getResponse}, nil
	case "SET":
		setResponse, err := handleSet(command)
		if err != nil {
			return CommandResponse{}, err
		}
		return CommandResponse{setResponse}, nil
	default:
		// err := fmt.Errorf("Not supported command: %s", command.CommandType)
		// return CommandResponse{}, err
		// TODO return PONG until a RESP error handling is available
		pingResponse := handlePing()
		return CommandResponse{pingResponse}, nil
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
