package exitcode_test

import (
	"errors"
	"testing"

	"strand/internal/exitcode"
)

func TestExitCodes(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected int
	}{
		{
			name:     "Exit 0: Success",
			err:      nil,
			expected: exitcode.Success,
		},
		{
			name:     "Exit 1: Unclassified error",
			err:      errors.New("some random error"),
			expected: exitcode.Unclassified,
		},
		{
			name:     "Exit 2: Invalid syntax / invalid config",
			err:      exitcode.New("invalid_syntax", exitcode.InvalidSyntax, "invalid yaml", nil),
			expected: exitcode.InvalidSyntax,
		},
		{
			name:     "Exit 3: Validate rejected (binlog_config_invalid)",
			err:      exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected, "missing binlog row", nil),
			expected: exitcode.Rejected,
		},
		{
			name:     "Exit 4: Fenced / Pipeline locked",
			err:      exitcode.New(exitcode.ErrPipelineLocked, exitcode.Fenced, "locked", nil),
			expected: exitcode.Fenced,
		},
		{
			name:     "Exit 5: Data error (log_gap)",
			err:      exitcode.New(exitcode.ErrLogGap, exitcode.DataError, "log missing", nil),
			expected: exitcode.DataError,
		},
		{
			name:     "Exit 10: Operator decision needed (promote_needs_decision)",
			err:      exitcode.New(exitcode.ErrPromoteNeedsDecision, exitcode.NeedsDecision, "needs decision", nil),
			expected: exitcode.NeedsDecision,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code := exitcode.GetExitCode(tt.err)
			if code != tt.expected {
				t.Errorf("GetExitCode(%v) = %d, expected %d", tt.err, code, tt.expected)
			}
		})
	}
}
