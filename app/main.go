package main

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
	"regexp"
	"strings"
	"time"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

func log(message string) {
	now := time.Now().Truncate(time.Second)
	logMessage := fmt.Sprintf("[%s] %s", now.Format("2006-01-02 15:04:05"), message)
	fmt.Println(logMessage)
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	log("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		log("Failed to bind to port 6379")
		os.Exit(1)
	}
	// Ensure to close connection after exit
	defer l.Close()
	
	for {
		con, err := l.Accept()
		log("Opening new connection")
		if err != nil {
			log(fmt.Sprintf("Error connection accept: ", err.Error()))
		}
		handleClient(con)
	}
}

type CommandResponse struct {
	responseType string
	responseContent string
}

type Command struct {
	commandValue string
}

func commandResponseToString(r CommandResponse) string {
	return r.responseType + r.responseContent
}

func commandResponsesToString(responses []CommandResponse) string {
	s := make([]string, 0)
	for _, response := range responses {
		responseString := commandResponseToString(response)
		log(fmt.Sprintf("Response string: %s", responseString))
		s = append(s, responseString)
	}
	commandResponseString := strings.Join(s, "\r\n")
	return commandResponseString + "\r\n"
}

func handleCommand(command string) *CommandResponse {
	switch command {
		case "PING":
			pingResponse := handlePing()
			return &pingResponse
		default:
			return nil
	}
}

func handlePing() CommandResponse {
	return CommandResponse{"+", "PONG"}
}

// TODO proper command parsing!!!!

func handleRequest(requestData []byte) string {
	ignore := regexp.MustCompile(`^[*$]`) // ignore comamnd data type for now
	splitData := bytes.Split(requestData, []byte("\r\n"))
	commandResponses := []CommandResponse{}
	for _, data := range splitData {
		command := string(data)
		if (ignore.MatchString(command)) {
			log(fmt.Sprintf("Ignoring command part: %s", command))
			continue
		} else {
			log(fmt.Sprintf("Processing command: %s", command))
			commandResponse := handleCommand(strings.ToUpper(command))
			if commandResponse != nil {
				commandResponses = append(commandResponses, *commandResponse)
			}
		}
	}
	responseString := commandResponsesToString(commandResponses)
	return responseString
}

func handleClient(conn net.Conn) {
	defer conn.Close()
	
	for {
		buf := make([]byte, 256)
		n, readErr := conn.Read(buf)
		if readErr != nil {
			if readErr == io.EOF {
				log("EOF detected. Closing connection")
				break
			}
			log(fmt.Sprintf("Error reading client: ", readErr.Error()))
			return
		}
		log(fmt.Sprintf("Recieved data: ", buf[:n]))
		log(fmt.Sprintf("Recieved data (str): ", string(buf[:n])))
		response := handleRequest(buf)
		
		log(fmt.Sprintf("Command Response: %s", response))
		_, writeErr := conn.Write([]byte(response))
		if writeErr != nil {
			log(fmt.Sprintf("Error writing client: ", writeErr.Error()))
		}
	}
}
