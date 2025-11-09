package main

import "testing"

func TestParseBulkStrings(t *testing.T) {
	// command example *2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n
	var tests = []struct {
		name  string
		input []byte
		want  RespContent
	}{
		{
			name:  "Empty bulk string should be parsed",
			input: []byte("$0\r\n\r\n"),
			want:  RespContent{value: "", dataType: BulkString},
		},
		{
			name:  "Bulk string should be parsed",
			input: []byte("$5\r\nhello\r\n"),
			want:  RespContent{value: "hello", dataType: BulkString},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ans, err := parseBulkString(tt.input)
			if err != nil {
				t.Errorf("result expected, but err got: %s", err.Error())
			}

			if (ans.value != tt.want.value) || (ans.dataType != tt.want.dataType) {
				t.Errorf("got %v, want %v", ans, tt.want)
			}
		})
	}
}

func TestParseArray(t *testing.T) {
	// command example *2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n
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
				{value: "PING", dataType: BulkString},
			},
		},
		{
			name:  "2 sized array should be parsed",
			input: []byte("*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n"),
			want: []RespContent{
				{value: "LLEN", dataType: BulkString},
				{value: "mylist", dataType: BulkString},
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
