package command

import (
	"maps"
	"testing"
)

func sameXaddCommandResults(f *XAddCommand, s *XAddCommand) bool {
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

func sameXrangeCommandResults(f *XRangeCommand, s *XRangeCommand) bool {
	// compare simple values
	return f.StreamKey == s.StreamKey &&
		f.StartMillisecondsTime == s.StartMillisecondsTime &&
		f.StartSequenceNumber == s.StartSequenceNumber &&
		f.EndMillisecondsTime == s.EndMillisecondsTime &&
		f.EndSequenceNumber == s.EndSequenceNumber
}

func TestParseXaddCommand(t *testing.T) {
	var tests = []struct {
		name  string
		input Command
		want  XAddCommand
	}{
		// simple XADD command
		{
			name: "XADD command should be parsed",
			input: Command{CommandType: "XADD", CommandValues: []string{
				"stream-key", "1-1", "foo", "bar",
			}},
			want: XAddCommand{
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
			want: XAddCommand{
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
			want: XAddCommand{
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

func TestParseXRangeCommand(t *testing.T) {
	var tests = []struct {
		name  string
		input Command
		want  XRangeCommand
	}{
		// XRANGE command
		{
			name: "XRANGE default sequence number",
			input: Command{CommandType: "XRANGE", CommandValues: []string{
				"stream-key", "1526985054069", "1526985054079",
			}},
			want: XRangeCommand{

				StreamKey:             "stream-key",
				StartMillisecondsTime: int64(1526985054069),
				StartSequenceNumber:   0,
				EndMillisecondsTime:   1526985054079,
				EndSequenceNumber:     0,
			},
		},
		{
			name: "XRANGE start sequence number defined",
			input: Command{CommandType: "XRANGE", CommandValues: []string{
				"stream-key", "1526985054069-1", "1526985054079",
			}},
			want: XRangeCommand{

				StreamKey:             "stream-key",
				StartMillisecondsTime: int64(1526985054069),
				StartSequenceNumber:   1,
				EndMillisecondsTime:   1526985054079,
				EndSequenceNumber:     0,
			},
		},
		{
			name: "XRANGE end sequence number defined",
			input: Command{CommandType: "XRANGE", CommandValues: []string{
				"stream-key", "1526985054069", "1526985054079-10",
			}},
			want: XRangeCommand{

				StreamKey:             "stream-key",
				StartMillisecondsTime: int64(1526985054069),
				StartSequenceNumber:   0,
				EndMillisecondsTime:   1526985054079,
				EndSequenceNumber:     10,
			},
		},
		{
			name: "XRANGE sequence numbers defined",
			input: Command{CommandType: "XRANGE", CommandValues: []string{
				"stream-key", "1526985054069-10", "1526985054079-20",
			}},
			want: XRangeCommand{

				StreamKey:             "stream-key",
				StartMillisecondsTime: int64(1526985054069),
				StartSequenceNumber:   10,
				EndMillisecondsTime:   1526985054079,
				EndSequenceNumber:     20,
			},
		},
		{
			name: "XRANGE start defined by - command",
			input: Command{CommandType: "XRANGE", CommandValues: []string{
				"stream-key", "-", "1526985054079-20",
			}},
			want: XRangeCommand{

				StreamKey:             "stream-key",
				StartMillisecondsTime: int64(0),
				StartSequenceNumber:   0,
				EndMillisecondsTime:   1526985054079,
				EndSequenceNumber:     20,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ans, err := parseXrangeCommand(&tt.input)
			if err != nil {
				t.Errorf("result expected, but err got: %s", err.Error())
			}

			if !sameXrangeCommandResults(&ans, &tt.want) {
				t.Errorf("got %v, want %v", ans, tt.want)
			}
		})
	}
}
