package main

import (
	"fmt"
)

type Command struct {
	commandValue string
}

type CommandResponse struct {
	responseType string
	responseContent string
}

func commandResponseToString(response CommandResponse) string {
	responseString := fmt.Sprintf("+%s\r\n", response.responseContent)
	return responseString
}

func handleCommandPing() CommandResponse {
	return CommandResponse{"+", "PONG"}
}

func parseCommnads(unparsedRequest string) []Command {
	commands := []Command{
		Command{
			commandValue: "PING",
		},
	}
	return commands
}

func processCommand(command Command) *CommandResponse {
	switch command.commandValue {
		case "PING":
			pingResponse := handleCommandPing()
			return &pingResponse
		default:
			return nil
	}
}