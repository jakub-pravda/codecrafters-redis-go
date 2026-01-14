package respparser

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/internal/utils"
)

type RespDataType byte

const (
	TypeArray        RespDataType = '*'
	TypeBulkString   RespDataType = '$'
	TypeInteger      RespDataType = ':'
	TypeSimpleError  RespDataType = '-'
	TypeSimpleString RespDataType = '+'
)

type RespData interface {
	Type() RespDataType
	String() string
	DebugString() string
}

type SimpleString struct {
	Value string
}

func (s SimpleString) Type() RespDataType  { return TypeSimpleString }
func (s SimpleString) String() string      { return s.Value }
func (s SimpleString) DebugString() string { return fmt.Sprintf("Simple string: %s", s.Value) }

type Integer struct {
	Value int
}

func (s Integer) Type() RespDataType { return TypeInteger }
func (s Integer) String() string     { return strconv.Itoa(s.Value) }
func (s Integer) DebugString() string {
	numStr := strconv.Itoa(s.Value)
	return fmt.Sprintf("Integer: %s", numStr)
}

type SimpleError struct {
	Value string
}

func (s SimpleError) Type() RespDataType { return TypeSimpleError }
func (s SimpleError) String() string     { return s.Value }
func (s SimpleError) DebugString() string {
	return fmt.Sprintf("Simple error: %s", s.Value)
}

type BulkString struct {
	Value  string
	IsNull bool
}

func (s BulkString) Type() RespDataType { return TypeBulkString }
func (s BulkString) String() string     { return s.Value }
func (s BulkString) DebugString() string {
	return fmt.Sprintf("Bulk string: %s", s.Value)
}

type Array struct {
	Items  []RespData
	IsNull bool
}

func (a Array) Type() RespDataType { return TypeArray }
func (a Array) String() string {
	itemsString := make([]string, len(a.Items))
	for n, respData := range a.Items {
		itemsString[n] = respData.String()
	}
	return fmt.Sprintf("[%s]", strings.Join(itemsString, ","))
}
func (a Array) DebugString() string {
	itemsString := make([]string, len(a.Items))
	for n, respData := range a.Items {
		itemsString[n] = respData.String()
	}
	return fmt.Sprintf("Array: [%s]", strings.Join(itemsString, ","))
}

func Serialize(data RespData) ([]byte, error) {

	switch d := data.(type) {
	case Array:
		b, err := SerializeArray(d)
		if err != nil {
			utils.Log(fmt.Sprintf("(RESP Serialize) Array serialization error %s", err.Error()))
			return []byte{}, err
		} else {
			return b, nil
		}
	case BulkString:
		return SerializeBulkString(d), nil
	case Integer:
		return SerializeInteger(d), nil
	case SimpleString:
		return SerializeSimpleString(d), nil
	case SimpleError:
		return SerializeSimpleError(d), nil
	default:
		err := errors.New("(RESP Serialize) Unsupported resp data type")
		return []byte{}, err
	}
}

func SerializeArray(r Array) ([]byte, error) {
	var buf bytes.Buffer
	arrayLengthString := strconv.Itoa(len(r.Items))

	// array definition
	buf.WriteByte(byte(TypeArray))

	if r.IsNull {
		buf.WriteString("-1")
		buf.WriteString(string(respSeparator))
		return buf.Bytes(), nil
	}

	buf.WriteString(arrayLengthString)
	buf.WriteString(string(respSeparator))

	for _, respData := range r.Items {
		serialized, err := Serialize(respData)
		if err != nil {
			utils.Log(fmt.Sprintf("(SerializeArray) Serialization error %s", err.Error()))
			return []byte{}, err
		}
		buf.Write(serialized)
	}

	return buf.Bytes(), nil
}

func SerializeBulkString(r BulkString) []byte {
	var buf bytes.Buffer
	buf.WriteByte(byte(TypeBulkString))

	if r.IsNull {
		buf.WriteString("-1")
		buf.WriteString(string(respSeparator))
		return buf.Bytes()
	}

	bulkLengthString := strconv.Itoa(len(r.Value))

	// bulk string definition
	buf.WriteString(bulkLengthString)
	buf.WriteString(string(respSeparator))

	// bulk string value
	buf.WriteString(r.Value)
	buf.WriteString(string(respSeparator))

	return buf.Bytes()
}

func SerializeInteger(r Integer) []byte {
	var buf bytes.Buffer
	intString := strconv.Itoa(r.Value)

	buf.WriteByte(byte(TypeInteger))
	buf.WriteString(intString)
	buf.WriteString(string(respSeparator))

	return buf.Bytes()
}

func SerializeSimpleString(r SimpleString) []byte {
	var buf bytes.Buffer

	buf.WriteByte(byte(TypeSimpleString))
	buf.WriteString(r.Value)
	buf.WriteString(string(respSeparator))

	return buf.Bytes()
}

func SerializeSimpleError(r SimpleError) []byte {
	var buf bytes.Buffer

	buf.WriteByte(byte(TypeSimpleError))
	buf.WriteString(r.Value)
	buf.WriteString(string(respSeparator))

	return buf.Bytes()
}

var respSeparator = []byte("\r\n")

func Deserialize(r *bufio.Reader) (RespData, error) {
	dataType, err := r.Peek(1)
	if err != nil {
		utils.Log(fmt.Sprintf("(Deserialize) Read buffer peek error %s", err.Error()))
		return SimpleError{}, err
	}

	switch dataType[0] {
	case byte(TypeSimpleString):
		s, err := DeserializeSimpleString(r)
		if err != nil {
			return SimpleString{}, err
		} else {
			return s, nil
		}
	case byte(TypeInteger):
		i, err := DeserializeInteger(r)
		if err != nil {
			return Integer{}, err
		} else {
			return i, nil
		}
	case byte(TypeSimpleError):
		e, err := DeserializeSimpleError(r)
		if err != nil {
			return SimpleError{}, err
		} else {
			return e, nil
		}
	case byte(TypeBulkString):
		b, err := DeserializeBulkString(r)
		if err != nil {
			return BulkString{}, err
		} else {
			return b, nil
		}
	case byte(TypeArray):
		a, err := DeserializeArray(r)
		if err != nil {
			return BulkString{}, err
		} else {
			return a, nil
		}
	default:
		err := errors.New("(RESP Deserialize) Unsupported deserializer")
		return SimpleError{}, err
	}
}

func DeserializeArray(r *bufio.Reader) (Array, error) {
	nextLine, _, err := r.ReadLine()

	if err != nil {
		utils.Log(fmt.Sprintf("(DeserializeArray) Next line read error %s", err.Error()))
		return Array{}, nil
	} else if nextLine[0] != byte(TypeArray) {
		return Array{}, errors.New("Not an array")
	}

	numOfElements, err := strconv.Atoi(string(nextLine[1:]))
	if err != nil {
		return Array{}, errors.New("Array length must be an integer")
	}

	array := Array{
		Items: make([]RespData, numOfElements),
	}

	if numOfElements == 0 {
		return array, nil
	}

	for n := range numOfElements {
		respData, err := Deserialize(r)
		if err != nil {
			if err == io.EOF {
				msg := "(DeserializeArray) EOF detected before iterating through number of expected items"
				incompleteErr := errors.New(msg)
				utils.Log(msg)
				return array, incompleteErr
			} else {
				utils.Log(fmt.Sprintf("(DeserializeArray) Deserialization ends with error error %s", err.Error()))
				return array, err
			}
		}
		utils.Log(fmt.Sprintf("(DeserializeArray) Array item deserialized %v", respData))
		array.Items[n] = respData
	}
	return array, nil
}

func DeserializeBulkString(r *bufio.Reader) (BulkString, error) {
	nextLine, _, err := r.ReadLine()
	if err != nil {
		utils.Log(fmt.Sprintf("(DeserializeBulkString) Next line read error %s", err.Error()))
		return BulkString{}, nil
	} else if nextLine[0] != byte(TypeBulkString) {
		return BulkString{}, errors.New("Not a bulk string")
	}

	bulkStringLength, err := strconv.Atoi(string(nextLine[1:]))
	if err != nil {
		return BulkString{}, errors.New("Bulk string length must be an integer")
	}

	if bulkStringLength == 0 {
		return BulkString{}, nil
	}

	// bulk string content
	nextLine, _, err = r.ReadLine()
	if err != nil {
		utils.Log(fmt.Sprintf("(DeserializeBulkString) Next line read error %s", err.Error()))
		return BulkString{}, nil
	}

	if len(nextLine) != bulkStringLength {
		err := fmt.Errorf("ERROR (DeserializeBulkString) Bulk string length check failed. Expected: %d, got: %d", bulkStringLength, len(nextLine))
		utils.Log(err.Error())

		return BulkString{}, err
	}

	return BulkString{
		Value: string(nextLine),
	}, nil
}

func DeserializeInteger(r *bufio.Reader) (Integer, error) {
	nextLine, _, err := r.ReadLine()
	if err != nil {
		utils.Log(fmt.Sprintf("(DeserializeInteger) Next line read error %s", err.Error()))
		return Integer{}, nil
	} else if nextLine[0] != byte(TypeInteger) {
		return Integer{}, errors.New("Not an integer")
	}

	intValue, err := strconv.Atoi(string(nextLine[1:]))
	if err != nil {
		return Integer{}, errors.New("Integer must be an valid number")
	}

	return Integer{Value: intValue}, nil
}

func DeserializeSimpleString(r *bufio.Reader) (SimpleString, error) {
	nextLine, _, err := r.ReadLine()
	if err != nil {
		utils.Log(fmt.Sprintf("(DeserializeSimpleString) Next line read error %s", err.Error()))
		return SimpleString{}, nil
	} else if nextLine[0] != byte(TypeSimpleString) {
		return SimpleString{}, errors.New("Not a simple string")
	}

	simpleString := string(nextLine[1:])
	return SimpleString{Value: simpleString}, nil
}

func DeserializeSimpleError(r *bufio.Reader) (SimpleError, error) {
	nextLine, _, err := r.ReadLine()
	if err != nil {
		utils.Log(fmt.Sprintf("(DeserializeSimpleError) Next line read error %s", err.Error()))
		return SimpleError{}, nil
	} else if nextLine[0] != byte(TypeSimpleError) {
		return SimpleError{}, errors.New("Not a simple error")
	}

	simpleError := string(nextLine[1:])
	return SimpleError{Value: simpleError}, nil
}
