package command

import (
	"maps"
	"testing"
)

func sameXaddCommandResults(f *XaddCommand, s *XaddCommand) bool {
	// compare simple values
	if f.StreamKey != s.StreamKey ||
		f.EntryIdMillisecondsTime != s.EntryIdMillisecondsTime ||
		f.EntryIdSequenceNumber != s.EntryIdSequenceNumber ||
		f.AutoEntryId != s.AutoEntryId {
		return false
	}

	// compare maps
	return maps.Equal(f.FieldValues, s.FieldValues)
}

func TestParseXaddCommand(t *testing.T) {
	var tests = []struct {
		name  string
		input Command
		want  XaddCommand
	}{
		// simple XADD command
		{
			name: "XADD command should be parsed",
			input: Command{CommandType: "XADD", CommandValues: []string{
				"stream-key", "1-1", "foo", "bar",
			}},
			want: XaddCommand{
				StreamKey:               "stream-key",
				EntryIdMillisecondsTime: 1,
				EntryIdSequenceNumber:   1,
				FieldValues:             map[string]string{"foo": "bar"},
			},
		},
		// XADD command - partially generated entry ID
		{
			name: "XADD command with partially generated entry ID should be parsed",
			input: Command{CommandType: "XADD", CommandValues: []string{
				"stream-key", "10-*", "foo", "bar",
			}},
			want: XaddCommand{
				StreamKey:               "stream-key",
				EntryIdMillisecondsTime: 10,
				FieldValues:             map[string]string{"foo": "bar"},
				AutoEntryId:             PartiallyGeneratedEntryId,
			},
		},
		// XADD command - fully generated entry ID
		{
			name: "XADD command with fully generated entry ID should be parsed",
			input: Command{CommandType: "XADD", CommandValues: []string{
				"stream-key", "*", "foo", "bar",
			}},
			want: XaddCommand{
				StreamKey:   "stream-key",
				FieldValues: map[string]string{"foo": "bar"},
				AutoEntryId: FullyGeneratedEntryId,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ans, err := parseXaddCommand(&tt.input)
			if err != nil {
				t.Errorf("result expected, but err got: %s", err.Error())
			}

			if !sameXaddCommandResults(&ans, &tt.want) {
				t.Errorf("got %v, want %v", ans, tt.want)
			}
		})
	}
}
