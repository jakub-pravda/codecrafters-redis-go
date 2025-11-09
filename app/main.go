package main

import (
	"fmt"
	"io"
	"net"
	"os"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	log("Logs from your program will appear here!")

	// Init event loop
	eventLoop := CommandEventLoop{
		mainTask:     make(chan Task, 10),
		commandQueue: make(chan Task, 10),
		stop:         make(chan bool),
	}
	wg := InitEventLoop(&eventLoop, 10)

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		log("Failed to bind to port 6379")
		os.Exit(1)
	}

	defer func() {
		l.Close()
		StopEventLoop(&eventLoop)
		wg.Wait()
	}()

	for {
		con, err := l.Accept()
		log("Opening new connection")
		if err != nil {
			log(fmt.Sprintf("Error connection accept: %s", err.Error()))
		}
		// run connection handler in separate goroutine
		go handleConnection(con, &eventLoop)
	}
}

func handleCommandRequest(requestCommands []byte) CommandResponse {
	command, _ := parseCommand(requestCommands) // TODO err handling

	log(fmt.Sprintf("Command: %s", command))
	commandResponse, _ := processCommand(command) // TODO error handling
	log(fmt.Sprintf("Command %s result: %s", command.commandType, commandResponse.responseContent))

	return commandResponse
}

func handleConnection(conn net.Conn, eventLoop *CommandEventLoop) {
	defer conn.Close()

	for {
		buf := make([]byte, 1024)
		n, readErr := conn.Read(buf)
		if readErr != nil {
			if readErr == io.EOF {
				log("EOF detected. Closing connection")
				break
			} else {
				log(fmt.Sprintf("Error reading client: %s", readErr.Error()))
				break
			}
		}

		command := buf[:n]

		log(fmt.Sprintf("Recieved data: %s", command))
		log(fmt.Sprintf("Recieved data (str): %s", string(command)))

		Add(eventLoop, &Task{
			// TODO split command processing and client write
			MainTask: func() {
				commandResult := handleCommandRequest(command)
				responseString := commandResponseToString(commandResult)
				log(fmt.Sprintf("Sending response: %s", responseString))
				_, writeErr := conn.Write([]byte(responseString))
				if writeErr != nil {
					log(fmt.Sprintf("Error writing client: %s", writeErr.Error()))
				}
			},
			IsBlocking: true,
		})
	}
}
