package respparser

import (
	"bufio"
	"bytes"
	"fmt"
	"testing"

	"github.com/codecrafters-io/redis-starter-go/app/internal/utils"
)

func TestDecodeBulkStrings(t *testing.T) {
	var tests = []struct {
		name  string
		input []byte
		want  BulkString
	}{
		{
			name:  "Empty bulk string should be parsed",
			input: []byte("$0\r\n\r\n"),
			want:  BulkString{Value: ""},
		},
		{
			name:  "Bulk string should be parsed",
			input: []byte("$5\r\nhello\r\n"),
			want:  BulkString{Value: "hello"},
		},
		{
			name:  "Bulk string with special char should be parsed",
			input: []byte("$1\r\n*\r\n"),
			want:  BulkString{Value: "*"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			utils.Log(fmt.Sprintf("*** Running test: %s***", tt.name))
			r := bufio.NewReader(bytes.NewReader(tt.input))
			ans, err := DeserializeBulkString(r)
			if err != nil {
				t.Errorf("result expected, but err got: %s", err.Error())
			}

			if ans.Value != tt.want.Value {
				t.Errorf("got %v, want %v", ans, tt.want)
			}
		})
	}
}

func TestEncodeBulkStrings(t *testing.T) {
	var tests = []struct {
		name  string
		input BulkString
		want  []byte
	}{
		{
			name:  "Empty bulk string should be parsed",
			input: BulkString{Value: ""},
			want:  []byte("$0\r\n\r\n"),
		},
		{
			name:  "Bulk string should be parsed",
			input: BulkString{Value: "hello"},
			want:  []byte("$5\r\nhello\r\n"),
		},
		{
			name:  "Null Bulk string should be parsed",
			input: BulkString{Value: "", IsNull: true},
			want:  []byte("$-1\r\n"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ans, err := Serialize(tt.input)
			if err != nil {
				t.Errorf("result expected, but err got: %s", err.Error())
			}

			if !bytes.Equal(ans, tt.want) {
				t.Errorf("got %v, want %v", string(ans), string(tt.want))
			}
		})
	}
}

func TestEncodeSimpleStrings(t *testing.T) {
	var tests = []struct {
		name  string
		input SimpleString
		want  []byte
	}{
		{
			name:  "Simple string should be parsed",
			input: SimpleString{Value: "hello"},
			want:  []byte("+hello\r\n"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			utils.Log(fmt.Sprintf("*** Running test: %s***", tt.name))
			ans, err := Serialize(tt.input)
			if err != nil {
				t.Errorf("result expected, but err got: %s", err.Error())
			}
			if !bytes.Equal(ans, tt.want) {
				t.Errorf("got %v, want %v", ans, tt.want)
			}
		})
	}
}

func TestDeserializeArray(t *testing.T) {
	var tests = []struct {
		name  string
		input []byte
		want  Array
	}{
		{
			name:  "0 sized array should be parsed",
			input: []byte("*0\r\n"),
			want:  Array{},
		},
		{
			name:  "1 sized array should be parsed",
			input: []byte("*1\r\n$4\r\nPING\r\n"),
			want: Array{
				Items: []RespData{
					BulkString{Value: "PING"},
				},
			},
		},
		{
			name:  "2 sized array should be parsed",
			input: []byte("*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n"),
			want: Array{
				Items: []RespData{
					BulkString{Value: "LLEN"},
					BulkString{Value: "mylist"},
				},
			},
		},
		{
			name:  "Inner array should be parsed",
			input: []byte("*2\r\n*2\r\n$15\r\n1526985054069-0\r\n*4\r\n$11\r\ntemperature\r\n$2\r\n36\r\n$8\r\nhumidity\r\n$2\r\n95\r\n*2\r\n$15\r\n1526985054079-0\r\n*4\r\n$11\r\ntemperature\r\n$2\r\n37\r\n$8\r\nhumidity\r\n$2\r\n94\r\n"),
			want: Array{
				Items: []RespData{
					// 1 ARRAY
					Array{
						Items: []RespData{
							BulkString{Value: "1526985054069-0"},
							Array{
								Items: []RespData{
									BulkString{Value: "temperature"},
									BulkString{Value: "36"},
									BulkString{Value: "humidity"},
									BulkString{Value: "95"},
								},
							},
						},
					},
					// 2 ARRAY
					Array{
						Items: []RespData{
							BulkString{Value: "1526985054079-0"},
							Array{
								Items: []RespData{
									BulkString{Value: "temperature"},
									BulkString{Value: "37"},
									BulkString{Value: "humidity"},
									BulkString{Value: "94"},
								},
							},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			utils.Log(fmt.Sprintf("*** Running test: %s***", tt.name))
			r := bufio.NewReader(bytes.NewReader(tt.input))
			ans, err := DeserializeArray(r)
			if err != nil {
				t.Errorf("result expected, but err got: %s", err.Error())
			}
			utils.Log(fmt.Sprintf("(TEST) Array deserialize result %v", ans.String()))
			if ans.String() != tt.want.String() {
				t.Errorf("got %v, want %v", ans.String(), tt.want.String())
			}
		})
	}
}

func TestSerializeArray(t *testing.T) {
	var tests = []struct {
		name  string
		input Array
		want  []byte
	}{
		{
			name: "Inner array should be parsed",
			input: Array{
				Items: []RespData{
					// 1 ARRAY
					Array{
						Items: []RespData{
							BulkString{Value: "1526985054069-0"},
							Array{
								Items: []RespData{
									BulkString{Value: "temperature"},
									BulkString{Value: "36"},
									BulkString{Value: "humidity"},
									BulkString{Value: "95"},
								},
							},
						},
					},
					// 2 ARRAY
					Array{
						Items: []RespData{
							BulkString{Value: "1526985054079-0"},
							Array{
								Items: []RespData{
									BulkString{Value: "temperature"},
									BulkString{Value: "37"},
									BulkString{Value: "humidity"},
									BulkString{Value: "94"},
								},
							},
						},
					},
				},
			},
			want: []byte("*2\r\n*2\r\n$15\r\n1526985054069-0\r\n*4\r\n$11\r\ntemperature\r\n$2\r\n36\r\n$8\r\nhumidity\r\n$2\r\n95\r\n*2\r\n$15\r\n1526985054079-0\r\n*4\r\n$11\r\ntemperature\r\n$2\r\n37\r\n$8\r\nhumidity\r\n$2\r\n94\r\n"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			utils.Log(fmt.Sprintf("*** Running test: %s***", tt.name))
			ans, err := SerializeArray(tt.input)
			if err != nil {
				t.Errorf("result expected, but err got: %s", err.Error())
			}

			utils.Log(fmt.Sprintf("(TEST) Array serialize result %v", string(ans)))
			if !bytes.Equal(ans, tt.want) {
				t.Errorf("got %v, want %v", string(ans), string(tt.want))
			}
		})
	}
}
