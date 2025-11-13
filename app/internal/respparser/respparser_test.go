package respparser

import (
	"bytes"
	"testing"
)

func TestDecodeBulkStrings(t *testing.T) {
	var tests = []struct {
		name  string
		input []byte
		want  RespContent
	}{
		{
			name:  "Empty bulk string should be parsed",
			input: []byte("$0\r\n\r\n"),
			want:  RespContent{Value: "", DataType: BulkString},
		},
		{
			name:  "Bulk string should be parsed",
			input: []byte("$5\r\nhello\r\n"),
			want:  RespContent{Value: "hello", DataType: BulkString},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ans, err := decodeBulkString(tt.input)
			if err != nil {
				t.Errorf("result expected, but err got: %s", err.Error())
			}

			if (ans.Value != tt.want.Value) || (ans.DataType != tt.want.DataType) {
				t.Errorf("got %v, want %v", ans, tt.want)
			}
		})
	}
}

func TestEncodeBulkStrings(t *testing.T) {
	var tests = []struct {
		name  string
		input RespContent
		want  []byte
	}{
		{
			name:  "Empty bulk string should be parsed",
			input: RespContent{Value: "", DataType: BulkString},
			want:  []byte("$0\r\n\r\n"),
		},
		{
			name:  "Bulk string should be parsed",
			input: RespContent{Value: "hello", DataType: BulkString},
			want:  []byte("$5\r\nhello\r\n"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ans := EncodeBulkString(tt.input)
			if !bytes.Equal(ans, tt.want) {
				t.Errorf("got %v, want %v", ans, tt.want)
			}
		})
	}
}

func TestParseArray(t *testing.T) {
	var tests = []struct {
		name  string
		input []byte
		want  []RespContent
	}{
		{
			name:  "0 sized array should be parsed",
			input: []byte("*0\r\n"),
			want:  []RespContent{},
		},
		{
			name:  "1 sized array should be parsed",
			input: []byte("*1\r\n$4\r\nPING\r\n"),
			want: []RespContent{
				{Value: "PING", DataType: BulkString},
			},
		},
		{
			name:  "2 sized array should be parsed",
			input: []byte("*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n"),
			want: []RespContent{
				{Value: "LLEN", DataType: BulkString},
				{Value: "mylist", DataType: BulkString},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ans, err := ParseArray(tt.input)
			if err != nil {
				t.Errorf("result expected, but err got: %s", err.Error())
			}

			for n, cont := range ans {
				if cont != tt.want[n] {
					t.Errorf("got %v, want %v", cont, tt.want[n])
				}
			}
		})
	}
}
