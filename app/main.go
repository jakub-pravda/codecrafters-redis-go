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
	eventLoop := CommandEventLoop {
		mainTask: make(chan Task, 10),
		commandQueue: make(chan Task, 10),
		stop: make(chan bool),
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
			log(fmt.Sprintf("Error connection accept: ", err.Error()))
		}
		// run connectiuon handler in separate goroutine
		go handleConnection(con, &eventLoop)
	}
}

func handleCommandRequest(requestCommands []byte) []CommandResponse {
	commands := parseCommnads(string(requestCommands))
	commandResponses := make([]CommandResponse, 0, len(commands))
	for _, command := range commands {
		log(fmt.Sprintf("Command: %s", command))
		commandResponse := processCommand(command)
		log(fmt.Sprintf("Command %s result: %s", command.commandValue, commandResponse.responseContent))
		if commandResponse != nil {
			commandResponses = append(commandResponses, *commandResponse)
		}
	}
	return commandResponses
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
				log(fmt.Sprintf("Error reading client: ", readErr.Error()))
				break
			}
		}
		
		log(fmt.Sprintf("Recieved data: ", buf[:n]))
		log(fmt.Sprintf("Recieved data (str): ", string(buf[:n])))
		
		Add(eventLoop, &Task{
			MainTask: func() {
				commandResults := handleCommandRequest(buf)
				for _, commandResult := range commandResults {
					responseString := commandResponseToString(commandResult)
					log(fmt.Sprintf("Sending response: ", responseString))
					_, writeErr := conn.Write([]byte(responseString))
					if writeErr != nil {
						log(fmt.Sprintf("Error writing client: ", writeErr.Error()))
					}
				}	
			},
			IsBlocking: true,
		})
	}
}
