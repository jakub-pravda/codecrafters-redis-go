package main

import (
	"bufio"
	"fmt"
	"net"
	"os"

	"github.com/codecrafters-io/redis-starter-go/app/internal/command"
	"github.com/codecrafters-io/redis-starter-go/app/internal/eventloop"
	"github.com/codecrafters-io/redis-starter-go/app/internal/respparser"
	"github.com/codecrafters-io/redis-starter-go/app/internal/utils"
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

func handleCommandRequest(r *bufio.Reader) command.CommandResponse {
	cmd, err := command.ParseCommand(r) // TODO err handling
	if err != nil {
		return command.ErrorResponse(err)
	}

	utils.Log(fmt.Sprintf("(Request handler) Command: %s", cmd))

	commandHandler, err := command.GetCommandHandler(cmd)
	if err != nil {
		return command.ErrorResponse(err)
	}

	cmdResponse, err := commandHandler.Process()
	if err != nil {
		return command.ErrorResponse(err)
	}

	utils.Log(fmt.Sprintf("(Request handler) Command %s result: %s", cmd.CommandType, cmdResponse.String()))

	response := command.CommandResponse{
		Value: cmdResponse,
	}
	return response
}

func handleConnection(conn net.Conn, eventLoop *eventloop.CommandEventLoop) {
	defer conn.Close()

	for {
		commandReader := bufio.NewReader(conn)
		commandDataType, err := commandReader.Peek(1)
		if err != nil {
			utils.Log("(Connection handler) Can't read data from incomming connection")
			break
		}

		utils.Log(fmt.Sprintf("(Connection handler) Recieved new data with type: %v", commandDataType))

		eventloop.Add(eventLoop, &eventloop.Task{
			// TODO split command processing and client write
			MainTask: func() {
				cmdResult := handleCommandRequest(commandReader)
				utils.Log(fmt.Sprintf("(Connection handler) Sending response: %v", cmdResult.Value))

				encodedResp, serializationErr := respparser.Serialize(cmdResult.Value)
				if serializationErr != nil {
					utils.Log(fmt.Sprintf("(Connection handler) Error writing client: %s", serializationErr.Error()))
					// TODO return error!
				}

				_, writeErr := conn.Write(encodedResp)
				if writeErr != nil {
					utils.Log(fmt.Sprintf("(Connection handler) Error writing client: %s", writeErr.Error()))
					// TODO return error!
				}
			},
			IsBlocking: true,
		})
	}
}
