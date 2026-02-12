package command

import (
	"errors"
	"fmt"

	"github.com/codecrafters-io/redis-starter-go/app/internal/respparser"
	"github.com/codecrafters-io/redis-starter-go/app/internal/store"
	"github.com/codecrafters-io/redis-starter-go/app/internal/utils"
)

type RPushCommand struct {
	Key    string
	Values []string
}

func (c RPushCommand) Process() (respparser.RespData, error) {
	utils.Log(fmt.Sprintf("(RPushCommand) Processing list with key %s", c.Key))

	listStoreValue := store.ListStoreValue{
		Key:    c.Key,
		Values: c.Values,
	}

	numOfAddedElements := store.ListStoreLive.Append(listStoreValue)
	respInt := respparser.Integer{Value: numOfAddedElements}
	return respInt, nil
}

func parseRPushCommand(command *Command) (RPushCommand, error) {
	if command.CommandType != "RPUSH" {
		return RPushCommand{}, errors.New("Not a RPUSH")
	}

	rPushCommand := RPushCommand{
		// remark: length is len of command values - 1 (first element is always list key)
		Values: make([]string, len(command.CommandValues)-1),
	}

	for n, element := range command.CommandValues {
		if n == 0 {
			utils.Log(fmt.Sprintf("(RPushCommand)(parser) Parsing key %s", element))
			rPushCommand.Key = element
		} else {
			utils.Log(fmt.Sprintf("(RPushCommand)(parser) Parsing value %s", element))
			rPushCommand.Values[n-1] = element
		}
	}

	return rPushCommand, nil
}
