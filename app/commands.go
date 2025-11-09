package main

import (
	"errors"
	"fmt"
)

type Command struct {
	commandType   string
	commandValues []string
}

type CommandResponse struct {
	responseType    string
	responseContent string
}

func commandResponseToString(response CommandResponse) string {
	responseString := fmt.Sprintf("+%s\r\n", response.responseContent)
	return responseString
}

func handleCommandPing() CommandResponse {
	return CommandResponse{"+", "PONG"}
}

func processCommand(command *Command) (CommandResponse, error) {
	switch command.commandType {
	case "PING":
		pingResponse := handleCommandPing()
		return pingResponse, nil
	default:
		emptyResponse := CommandResponse{
			responseType:    "",
			responseContent: "",
		}
		return emptyResponse, errors.New("Invalid command")
	}
}
