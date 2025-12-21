package respparser

import (
	"bytes"
	"errors"
	"fmt"
	"regexp"
	"strconv"

	"github.com/codecrafters-io/redis-starter-go/app/internal/utils"
)

type RespDataType byte

const (
	Array        RespDataType = '*'
	BulkString   RespDataType = '$'
	Integer      RespDataType = ':'
	SimpleError  RespDataType = '-'
	SimpleString RespDataType = '+'
)

type RespContent struct {
	Value    string
	DataType RespDataType
	IsEmpty  bool
}

var respSeparator = []byte("\r\n")
var respNil = append([]byte("$-1"), respSeparator...)

func dataTypeDefinition(respContent []byte) *RespDataType {
	arrayRe := regexp.MustCompile(`^\*\d+$`)
	if arrayRe.Match(respContent) {
		result := Array
		return &result
	}

	bulkStringRe := regexp.MustCompile(`^\$\d+$`)
	if bulkStringRe.Match(respContent) {
		result := BulkString
		return &result
	}

	integerRe := regexp.MustCompile(`^\:\d+$`)
	if integerRe.Match(respContent) {
		result := Integer
		return &result
	}

	return nil
}

type DataContentHeader []byte
type DataContentTail [][]byte

// nextDataContent returns next content (head) and the rest of the command (tail)
func nextDataContent(content [][]byte) (DataContentHeader, DataContentTail, *RespDataType) {
	utils.Log(fmt.Sprintf("(Data content iterator) input: %v", content))
	var head []byte
	var tail [][]byte

	if len(content) == 0 {
		return nil, nil, nil
	} else if len(content) == 1 { // TODO refactor, don't like it
		head, tail = content[0], nil // empty array
	} else {
		head, tail = content[0], content[1:]
	}

	utils.Log(fmt.Sprintf("(Data content iterator) head: %s", head))
	utils.Log(fmt.Sprintf("(Data content iterator) tail: %s", tail))

	nextDataType := dataTypeDefinition(head)
	return head, tail, nextDataType
}

func getNumberOfArrayElements(arrayType []byte) (int, error) {
	if len(arrayType) == 0 || arrayType[0] != byte(Array) {
		return 0, errors.New("Not an array type")
	}

	numOfElements, err := strconv.Atoi(string(arrayType[1:]))
	if err != nil {
		return 0, errors.New("Array length must be an integer")
	}
	return numOfElements, err
}

func ParseArray(cmd []byte) ([]RespContent, error) {
	// check array

	arraySplit := bytes.Split(cmd, respSeparator)
	arrayId, arrayData, dataType := nextDataContent(arraySplit)
	if dataType == nil || *dataType != Array {
		return nil, errors.New("Not an array")
	}

	numOfElements, err := getNumberOfArrayElements(arrayId)
	if err != nil {
		return nil, fmt.Errorf("Error occurs during array parsing: %s", err.Error())
	} else if numOfElements == 0 {
		// empty array
		return []RespContent{}, nil
	}

	respContent := make([]RespContent, 0)
	nextIter := arrayData[:len(arrayData)-1] // drop last separator (empty byte)

	for len(nextIter) > 0 {
		next, tail, dataType := nextDataContent(nextIter)
		utils.Log(fmt.Sprintf("(Array parser) next element: %s", next))
		if dataType != nil && *dataType == BulkString {
			// Ge bulk string string content and join
			bulkStringSize := next
			bulkStringContent := tail[0]

			// Create valid bulk string (according to resp definition)
			bulkString := bytes.Join([][]byte{bulkStringSize, bulkStringContent}, respSeparator)
			bulkString = append(bulkString, respSeparator...)

			decodedBulkString, err := decodeBulkString(bulkString)
			if err != nil {
				return nil, fmt.Errorf("Can't parse bulk string: %s", err.Error())
			}
			utils.Log(fmt.Sprintf("(Array parser) Appending resp content (bulk string): %v", *decodedBulkString))
			respContent = append(respContent, *decodedBulkString)
			nextIter = tail[1:]
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
	stringSize := append([]byte{byte(BulkString)}, lenBytes...)

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
	// bulk string example: $<length>\r\n<data>\r\n
	utils.Log(fmt.Sprintf("(Bulk parser) input: %s", bulkString))
	if bulkString[0] != byte(BulkString) {
		return nil, errors.New("Not a bulk string")
	}

	splitBulkString := bytes.Split(bulkString, respSeparator)
	if len(splitBulkString) != 3 {
		// 3 elements are expected as golang splits $<length>\r\n<data>\r\n string into 3 parts
		return nil, fmt.Errorf("Not a valid bulk string length, expected 3, got %d", len(splitBulkString))
	}

	bulkStringSize := splitBulkString[0]
	bulkStringContent := splitBulkString[1]

	utils.Log(fmt.Sprintf("(Bulk parser) size part: %s", bulkStringSize))
	utils.Log(fmt.Sprintf("(Bulk parser) data part: %s", bulkStringContent))

	stringSize, err := strconv.Atoi(string(bulkStringSize[1:])) // remove data type prefix
	if err != nil {
		return nil, errors.New("Can't parse string length")
	}

	stringContent := string(bulkStringContent)
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
	result := append([]byte{byte(SimpleString)}, []byte(s)...)
	result = append(result, respSeparator...)

	return result
}

func encodeSimpleError(errorMessage string) []byte {
	result := append([]byte{byte(SimpleError)}, []byte(errorMessage)...)
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
