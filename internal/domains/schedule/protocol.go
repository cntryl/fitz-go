package schedule

import (
	"errors"
	"strings"
)

// Wire opcodes for Schedule domain (per CLIENT_SPEC.md). Values are low-byte uint8 equivalents.
const (
	ScheduleCreate uint8 = 244
	ScheduleCancel uint8 = 245
	ScheduleList   uint8 = 246
)

// Domain-specific errors.
var (
	ErrScheduleNotFound = errors.New("schedule not found")
)

// mapScheduleError maps a broker error message to a domain-specific Go error.
func mapScheduleError(msg string) error {
	l := strings.ToLower(msg)
	switch {
	case strings.Contains(l, "not found"):
		return ErrScheduleNotFound
	default:
		return errors.New(msg)
	}
}
