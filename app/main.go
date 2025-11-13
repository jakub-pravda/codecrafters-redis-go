package main

import (
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/internal/command"
	"github.com/codecrafters-io/redis-starter-go/app/internal/eventloop"
	"github.com/codecrafters-io/redis-starter-go/app/internal/utils"
	"io"
	"net"
	"os"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	utils.Log("Logs from your program will appear here!")

	// Init event loop
	eventLoop := eventloop.CommandEventLoop{
		MainTask:     make(chan eventloop.Task, 10),
		CommandQueue: make(chan eventloop.Task, 10),
		Stop:         make(chan bool),
	}
	wg := eventloop.InitEventLoop(&eventLoop, 10)

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		utils.Log("Failed to bind to port 6379")
		os.Exit(1)
	}

	defer func() {
		l.Close()
		eventloop.StopEventLoop(&eventLoop)
		wg.Wait()
	}()

	for {
		con, err := l.Accept()
		utils.Log("Opening new connection")
		if err != nil {
			utils.Log(fmt.Sprintf("Error connection accept: %s", err.Error()))
		}
		// run connection handler in separate goroutine
		go handleConnection(con, &eventLoop)
	}
}

func handleCommandRequest(requestCommands []byte) command.CommandResponse {
	cmd, _ := command.ParseCommand(requestCommands) // TODO err handling

	utils.Log(fmt.Sprintf("Command: %s", cmd))
	cmdResponse, _ := command.ProcessCommand(cmd) // TODO error handling
	utils.Log(fmt.Sprintf("Command %s result: %s", cmd.CommandType, cmdResponse.Value))

	return cmdResponse
}

func handleConnection(conn net.Conn, eventLoop *eventloop.CommandEventLoop) {
	defer conn.Close()

	for {
		buf := make([]byte, 1024)
		n, readErr := conn.Read(buf)
		if readErr != nil {
			if readErr == io.EOF {
				utils.Log("EOF detected. Closing connection")
				break
			} else {
				utils.Log(fmt.Sprintf("Error reading client: %s", readErr.Error()))
				break
			}
		}

		command := buf[:n]

		utils.Log(fmt.Sprintf("Recieved data: %s", command))
		utils.Log(fmt.Sprintf("Recieved data (str): %s", string(command)))

		eventloop.Add(eventLoop, &eventloop.Task{
			// TODO split command processing and client write
			MainTask: func() {
				cmdResult := handleCommandRequest(command)
				utils.Log(fmt.Sprintf("Sending response: %s", cmdResult.Value))
				_, writeErr := conn.Write(cmdResult.Value)
				if writeErr != nil {
					utils.Log(fmt.Sprintf("Error writing client: %s", writeErr.Error()))
				}
			},
			IsBlocking: true,
		})
	}
}
