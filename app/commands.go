package main

import (
	"strings"
)

type Command struct {
	commandType   string
	commandValues []string
}

type CommandResponse struct {
	value []byte
}

func handleCommandPing() CommandResponse {
	response := "+PONG\r\n" // TODO encode simple string!
	return CommandResponse{[]byte(response)}
}

func handleEcho(command *Command) CommandResponse {
	echoResponse := strings.Join(command.commandValues, " ")
	resp := RespContent{
		value:    echoResponse,
		dataType: BulkString,
	}
	encoded := encodeBulkString(resp)
	return CommandResponse{encoded}
}

func processCommand(command *Command) (CommandResponse, error) {
	switch strings.ToUpper(command.commandType) {
	case "PING":
		pingResponse := handleCommandPing()
		return pingResponse, nil
	case "ECHO":
		echoResponse := handleEcho(command)
		return echoResponse, nil
	default:
		// ping for now
		pingResponse := handleCommandPing()
		return pingResponse, nil
	}
}
