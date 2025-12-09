package command

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/internal/respparser"
	"github.com/codecrafters-io/redis-starter-go/app/internal/utils"
)

func parsePingCommand(command *Command) (PingCommand, error) {
	return PingCommand{}, nil
}

func parseEchoCommand(command *Command) (EchoCommand, error) {
	if command.CommandType != "ECHO" {
		return EchoCommand{}, errors.New("Not an ECHO")
	}

	echoCommand := EchoCommand{
		Message: strings.Join(command.CommandValues, " "),
	}
	return echoCommand, nil
}

func parseGetCommand(command *Command) (GetCommand, error) {
	if command.CommandType != "GET" {
		return GetCommand{}, errors.New("Not a GET")
	} else if len(command.CommandValues) != 1 {
		return GetCommand{}, errors.New("GET must contains only one value (key)")
	}

	getCommand := GetCommand{
		Key: command.CommandValues[0],
	}
	return getCommand, nil
}

func parseSetCommand(command *Command) (SetCommand, error) {
	if len(command.CommandValues) < 2 {
		return SetCommand{}, errors.New("(SET cmd) too few arguments. At least key and value expected")
	}

	setCommand := SetCommand{
		RecordExpirationMillis: math.MaxInt32,
	}

	for n, arg := range command.CommandValues {
		if n == 0 {
			setCommand.Key = arg
			continue
		} else if n == 1 {
			setCommand.Value = arg
			continue
		} else {
			switch arg {
			case "PX":
				// millisecond expiry
				nextElem := n + 1
				if nextElem >= len(command.CommandValues) {
					errMsg := "(SET cmd) PX key expects value, but it seems to be missing!"
					utils.Log(errMsg)
					return SetCommand{}, errors.New(errMsg)
				}

				pxValue := command.CommandValues[nextElem]
				pxValueInt, err := strconv.Atoi(pxValue)
				if err != nil {
					errMsg := fmt.Sprintf("(SET cmd) PX value is expected to be int, but got %s", pxValue)
					utils.Log(errMsg)
					return SetCommand{}, errors.New(errMsg)
				}
				setCommand.RecordExpirationMillis = pxValueInt

			case "EX":
				// second expiry
				nextElem := n + 1
				if nextElem >= len(command.CommandValues) {
					errMsg := "(SET cmd) EX key expects value, but it seems to be missing!"
					utils.Log(errMsg)
					return SetCommand{}, errors.New(errMsg)
				}

				exValue := command.CommandValues[nextElem]
				exValueInt, err := strconv.Atoi(exValue)
				if err != nil {
					errMsg := fmt.Sprintf("(SET cmd) EX value is expected to be int, but got %s", exValue)
					utils.Log(errMsg)
					return SetCommand{}, errors.New(errMsg)
				}
				setCommand.RecordExpirationMillis = exValueInt * 1000 // from sec to millis
			default:
				continue
			}
		}
	}
	return setCommand, nil
}

func parseTypeCommand(command *Command) (TypeCommand, error) {
	if command.CommandType != "TYPE" {
		return TypeCommand{}, errors.New("Not a TYPE")
	} else if len(command.CommandValues) != 1 {
		return TypeCommand{}, errors.New("(TYPE cmd) must contains only one value (key)")
	}

	typeCommand := TypeCommand{
		Key: command.CommandValues[0],
	}
	return typeCommand, nil
}

func parseXaddCommand(command *Command) (XaddCommand, error) {
	if len(command.CommandValues) < 4 {
		return XaddCommand{}, errors.New("(XADD cmd) Too few arguments. At least stream-key, entry-id, akey and value expected")
	}

	xaddCommand := XaddCommand{}

	keys := []string{}
	values := []string{}

	for n, arg := range command.CommandValues {
		if n == 0 {
			xaddCommand.StreamKey = arg
			continue
		} else if n == 1 {
			millisecondsTime, sequenceNumber, err := parseXaddCommandEntryId(arg)
			if err != nil {
				return XaddCommand{}, err
			} else {
				xaddCommand.EntryIdMillisecondsTime = millisecondsTime
				xaddCommand.EntryIdSequenceNumber = sequenceNumber
				continue
			}
		} else {
			if n%2 == 0 {
				// keys
				keys = append(keys, arg)
			} else {
				// values
				values = append(values, arg)
			}
		}
	}

	if len(keys) != len(values) {
		return XaddCommand{}, errors.New("(XADD cmd) number of keys must be same as number of values!")
	}

	keyValues := make(map[string]string, len(keys))
	for i, k := range keys {
		keyValues[k] = values[i]
	}

	xaddCommand.FieldValues = keyValues
	return xaddCommand, nil
}

func parseXaddCommandEntryId(entryId string) (int, int, error) {
	split := strings.Split(entryId, "-")
	if len(split) != 2 {
		// explicit entry id
		errMsg := fmt.Sprintf("(XADD cmd) EntryId required format '<millisecondsTime>-<sequenceNumber>', but got: %s", entryId)
		utils.Log(errMsg)
		return 0, 0, errors.New(errMsg)
	}

	millisecondsTime, err := strconv.Atoi(split[0])
	if err != nil {
		errMsg := fmt.Sprintf("(XADD cmd) EntryId millisecondsTime must be an integer, but got: %s", split[0])
		utils.Log(errMsg)
		return 0, 0, errors.New(errMsg)
	}

	sequenceNumber, err := strconv.Atoi(split[1])
	if err != nil {
		errMsg := fmt.Sprintf("(XADD cmd) EntryId sequenceNumber must be an integer, but got: %s", split[1])
		utils.Log(errMsg)
		return 0, 0, errors.New(errMsg)
	}
	return millisecondsTime, sequenceNumber, nil
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

func GetCommandHandler(command *Command) (CommandHandler, error) {
	switch strings.ToUpper(command.CommandType) {
	case "PING":
		return parsePingCommand(command)
	case "ECHO":
		return parseEchoCommand(command)
	case "GET":
		return parseGetCommand(command)
	case "SET":
		return parseSetCommand(command)
	case "TYPE":
		return parseTypeCommand(command)
	case "XADD":
		return parseXaddCommand(command)
	default:
		return PingCommand{}, errors.ErrUnsupported
	}
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
