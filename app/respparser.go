package main

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
)

const (
	Array      = '*'
	BulkString = '$'
	Integer    = ':'
)

type RespContent struct {
	value    string
	dataType byte
}

const respSeparator = "\r\n"

func isDataTypeDefinition(respContent []byte) bool {
	if len(respContent) == 0 {
		return false
	}
	switch respContent[0] {
	case Array, BulkString, Integer:
		return true
	default:
		return false
	}
}

func nextDataContent(respContent []byte) ([]byte, []byte) {
	log(fmt.Sprintf("(Data content iterator) input: %v", respContent))
	if len(respContent) == 0 {
		return nil, nil
	}

	// get separators index
	separatorIndex := strings.Index(string(respContent), respSeparator) + len(respSeparator)
	log(fmt.Sprintf("(Data content iterator) Separator index: %d", separatorIndex))

	next := respContent[:separatorIndex]
	log(fmt.Sprintf("(Data content iterator) next: %s", next))
	tail := respContent[separatorIndex:]
	log(fmt.Sprintf("(Data content iterator) tail: %s", tail))

	// check if next element is data type or data content
	isTailDataType := isDataTypeDefinition(tail)
	if !isTailDataType {
		// Not a data type, parse next content
		dataTypeContent, dataTypeContentTail := nextDataContent(tail)
		joinedNext := slices.Concat(next, dataTypeContent)
		return joinedNext, dataTypeContentTail
	} else {
		return next, tail
	}
}

func parseArrayContent(arrayType []byte) (int, error) {
	if arrayType[0] != Array {
		return 0, errors.New("Not an array")
	}

	separatorIndex := strings.Index(string(arrayType), respSeparator)
	// remove data prefix and separator suffix
	numOfElements, err := strconv.Atoi(string(arrayType[1:separatorIndex]))
	if err != nil {
		return 0, errors.New("Array length must be an integer")
	}
	return numOfElements, err
}

func ParseArray(cmd []byte) ([]RespContent, error) {
	// check array
	arrayId, arrayData := nextDataContent(cmd)

	numOfElements, err := parseArrayContent(arrayId)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Error occurs during array parsing: %s", err.Error()))
	}

	respContent := make([]RespContent, 0)

	nextIter := arrayData
	for len(nextIter) > 0 {
		next, tail := nextDataContent(nextIter)
		log(fmt.Sprintf("(Array parser) next element: %s", next))
		if next[0] == BulkString {
			// bulk string, get next element and process
			bulkString, err := decodeBulkString(next)
			if err != nil {
				return nil, errors.New(fmt.Sprintf("Can't parse bulk string: %s", err.Error()))
			}
			log(fmt.Sprintf("Appending resp content (bulk string): %v", *bulkString))
			respContent = append(respContent, *bulkString)
			nextIter = tail
		} else {
			return nil, errors.New("Unsupported resp data type")
		}
	}

	if numOfElements != len(respContent) {
		return nil, errors.New(fmt.Sprintf("Parse resp content has different length (%d) than expected (%d)", numOfElements, len(respContent)))
	} else {
		return respContent, nil
	}
}

func encodeBulkString(content RespContent) []byte {
	if content.dataType != BulkString {
		return nil
	}

	lenBytes := []byte(strconv.Itoa(len(content.value)))
	stringSize := append([]byte{BulkString}, lenBytes...)

	stringContent := []byte(content.value)
	sep := []byte(respSeparator)

	totalLen := len(stringSize) + len(sep) + len(stringContent) + len(sep)
	result := make([]byte, 0, totalLen)

	result = append(result, stringSize...)
	result = append(result, sep...)
	result = append(result, stringContent...)
	result = append(result, sep...)

	return result
}

func decodeBulkString(bulkString []byte) (*RespContent, error) {
	log(fmt.Sprintf("(Bulk parser) input: %v", bulkString))
	if bulkString[0] != byte(BulkString) {
		return nil, errors.New("Not a bulk string")
	}

	splitBulkString := bytes.Split(bulkString, []byte(respSeparator))
	log(fmt.Sprintf("(Bulk parser) size part: %s", splitBulkString[0]))
	log(fmt.Sprintf("(Bulk parser) data part: %s", splitBulkString[2]))

	stringSize, err := strconv.Atoi(string(splitBulkString[0][1:])) // remove data type prefix
	if err != nil {
		return nil, errors.New("Can't parse string length")
	}

	stringContent := string(splitBulkString[1])
	if len(stringContent) != stringSize {
		return nil, errors.New(fmt.Sprintf("Parsed content length (%d) not equal to bulk string content length (%d)", len(stringContent), stringSize))
	}

	respContent := RespContent{
		value:    stringContent,
		dataType: BulkString,
	}

	return &respContent, nil
}

func elementsToCommand(elements []RespContent) Command {
	if len(elements) == 0 {
		// TODO return error instead PING
		return Command{
			commandType:   "PING",
			commandValues: nil,
		}
	} else if len(elements) == 1 {
		return Command{
			commandType:   elements[0].value,
			commandValues: nil,
		}
	} else {
		commandType := elements[0].value
		commandValues := make([]string, 0)
		for _, content := range elements[1:] {
			commandValues = append(commandValues, content.value)
		}
		return Command{
			commandType:   commandType,
			commandValues: commandValues,
		}
	}
}

func parseCommand(cmd []byte) (*Command, error) {
	// A client sends the Redis server an array consisting of only bulk strings.
	// command example *2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n
	arrayElements, error := ParseArray(cmd)
	if error != nil {
		// TODO error
		return nil, error
	}

	command := elementsToCommand(arrayElements)
	return &command, nil
}
