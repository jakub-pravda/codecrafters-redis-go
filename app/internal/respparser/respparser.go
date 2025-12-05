package respparser

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/internal/utils"
	"slices"
	"strconv"
	"strings"
)

const (
	Array        = '*'
	BulkString   = '$'
	Integer      = ':'
	SimpleError  = '-'
	SimpleString = '+'
)

type RespContent struct {
	Value    string
	DataType byte
	IsEmpty  bool
}

var respSeparator = []byte("\r\n")
var respNil = append([]byte("$-1"), respSeparator...)

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
	utils.Log(fmt.Sprintf("(Data content iterator) input: %v", respContent))
	if len(respContent) == 0 {
		return nil, nil
	}

	// get separators index
	separatorIndex := strings.Index(string(respContent), string(respSeparator)) + len(respSeparator)
	utils.Log(fmt.Sprintf("(Data content iterator) Separator index: %d", separatorIndex))

	next := respContent[:separatorIndex]
	utils.Log(fmt.Sprintf("(Data content iterator) next: %s", next))
	tail := respContent[separatorIndex:]
	utils.Log(fmt.Sprintf("(Data content iterator) tail: %s", tail))

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

	separatorIndex := strings.Index(string(arrayType), string(respSeparator))
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
		return nil, fmt.Errorf("Error occurs during array parsing: %s", err.Error())
	}

	respContent := make([]RespContent, 0)

	nextIter := arrayData
	for len(nextIter) > 0 {
		next, tail := nextDataContent(nextIter)
		utils.Log(fmt.Sprintf("(Array parser) next element: %s", next))
		if next[0] == BulkString {
			// bulk string, get next element and process
			bulkString, err := decodeBulkString(next)
			if err != nil {
				return nil, fmt.Errorf("Can't parse bulk string: %s", err.Error())
			}
			utils.Log(fmt.Sprintf("Appending resp content (bulk string): %v", *bulkString))
			respContent = append(respContent, *bulkString)
			nextIter = tail
		} else {
			return nil, errors.New("Unsupported resp data type")
		}
	}

	if numOfElements != len(respContent) {
		return nil, fmt.Errorf("Parse resp content has different length (%d) than expected (%d)", numOfElements, len(respContent))
	} else {
		return respContent, nil
	}
}

// ** BULK STRING ***

func encodeBulkString(content RespContent) []byte {
	if content.DataType != BulkString {
		return nil
	}

	lenBytes := []byte(strconv.Itoa(len(content.Value)))
	stringSize := append([]byte{BulkString}, lenBytes...)

	stringContent := []byte(content.Value)

	totalLen := len(stringSize) + len(respSeparator) + len(stringContent) + len(respSeparator)
	result := make([]byte, 0, totalLen)

	result = append(result, stringSize...)
	result = append(result, respSeparator...)
	result = append(result, stringContent...)
	result = append(result, respSeparator...)

	return result
}

func decodeBulkString(bulkString []byte) (*RespContent, error) {
	utils.Log(fmt.Sprintf("(Bulk parser) input: %v", bulkString))
	if bulkString[0] != byte(BulkString) {
		return nil, errors.New("Not a bulk string")
	}

	splitBulkString := bytes.Split(bulkString, []byte(respSeparator))
	utils.Log(fmt.Sprintf("(Bulk parser) size part: %s", splitBulkString[0]))
	utils.Log(fmt.Sprintf("(Bulk parser) data part: %s", splitBulkString[2]))

	stringSize, err := strconv.Atoi(string(splitBulkString[0][1:])) // remove data type prefix
	if err != nil {
		return nil, errors.New("Can't parse string length")
	}

	stringContent := string(splitBulkString[1])
	if len(stringContent) != stringSize {
		return nil, fmt.Errorf("Parsed content length (%d) not equal to bulk string content length (%d)", len(stringContent), stringSize)
	}

	respContent := RespContent{
		Value:    stringContent,
		DataType: BulkString,
	}

	return &respContent, nil
}

// ** SIMPLE STRINGS ***

func encodeSimpleString(s string) []byte {
	result := append([]byte{SimpleString}, []byte(s)...)
	result = append(result, respSeparator...)

	return result
}

func encodeSimpleError(errorMessage string) []byte {
	result := append([]byte{SimpleError}, []byte(errorMessage)...)
	result = append(result, respSeparator...)

	return result
}

// ** content ***
func EncodeRespContent(content RespContent) []byte {
	if content.IsEmpty {
		return respNil
	}

	switch content.DataType {
	case BulkString:
		return encodeBulkString(content)
	case SimpleString:
		return encodeSimpleString(content.Value)
	case SimpleError:
		return encodeSimpleError(content.Value)
	default:
		return respNil
	}
}
