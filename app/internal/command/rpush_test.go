package command

import (
	"testing"
)

func TestParseRPush(t *testing.T) {
	var tests = []struct {
		name  string
		input Command
		want  RPushCommand
	}{
		{
			name: "RPUSH append single element",
			input: Command{CommandType: "RPUSH", CommandValues: []string{
				"key", "element",
			}},
			want: RPushCommand{
				Key:    "key",
				Values: []string{"element"},
			},
		},
		{
			name: "RPUSH append multiple elements",
			input: Command{CommandType: "RPUSH", CommandValues: []string{
				"key", "element-1", "element-2",
			}},
			want: RPushCommand{
				Key:    "key",
				Values: []string{"element-1", "element-2"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ans, err := parseRPushCommand(&tt.input)
			if err != nil {
				t.Errorf("ERROR result expected, but err got: %s", err.Error())
			}

			if ans.Key != tt.want.Key || !IsEqualSlice(ans.Values, tt.want.Values) {
				t.Errorf("ERROR got %v, want %v", ans, tt.want)
			}
		})
	}
}
