package store

import (
	"testing"
)

func TestListStore(t *testing.T) {
	var tests = []struct {
		name  string
		input ListStoreValue
		want  ListStoreValue
	}{
		{
			name: "Store and retrieve single value",
			input: ListStoreValue{
				Key:    "list-key",
				Values: []string{"list-value"},
			},
			want: ListStoreValue{
				Key:    "list-key",
				Values: []string{"list-value"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			inserted := ListStoreLive.Append(tt.input)
			if inserted != len(tt.input.Values) {
				t.Errorf("ERROR Expected same number of elements after insert: %d but got: %d", len(tt.input.Values), inserted)
			}

			// TODO retrieve test
		})
	}
}
