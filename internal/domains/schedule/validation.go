package schedule

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	coreerrors "github.com/cntryl/fitz-go/internal/core/errors"
)

type cronFieldSpec struct {
	name  string
	min   int
	max   int
	names map[string]int
}

var scheduleCronFields = []cronFieldSpec{
	{name: "minute", min: 0, max: 59},
	{name: "hour", min: 0, max: 23},
	{name: "day of month", min: 1, max: 31},
	{name: "month", min: 1, max: 12, names: map[string]int{
		"JAN": 1,
		"FEB": 2,
		"MAR": 3,
		"APR": 4,
		"MAY": 5,
		"JUN": 6,
		"JUL": 7,
		"AUG": 8,
		"SEP": 9,
		"OCT": 10,
		"NOV": 11,
		"DEC": 12,
	}},
	{name: "day of week", min: 0, max: 7, names: map[string]int{
		"SUN": 0,
		"MON": 1,
		"TUE": 2,
		"WED": 3,
		"THU": 4,
		"FRI": 5,
		"SAT": 6,
	}},
}

func validateCronExpression(expr string) error {
	if err := parseCronExpression(expr); err != nil {
		return coreerrors.NewDomainError(uint32(coreerrors.ScheduleInvalidCron), fmt.Sprintf("invalid cron expression: %v", err))
	}
	return nil
}

func parseCronExpression(expr string) error {
	fields := strings.Fields(expr)
	if len(fields) != len(scheduleCronFields) {
		return fmt.Errorf("expected exactly %d fields, found %d: %q", len(scheduleCronFields), len(fields), expr)
	}

	for index, field := range fields {
		if err := scheduleCronFields[index].validate(field); err != nil {
			return fmt.Errorf("%s field %q: %w", scheduleCronFields[index].name, field, err)
		}
	}

	return nil
}

func (spec cronFieldSpec) validate(field string) error {
	for _, part := range strings.Split(field, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			return errors.New("empty list item")
		}
		if err := spec.validatePart(part); err != nil {
			return err
		}
	}

	return nil
}

func (spec cronFieldSpec) validatePart(part string) error {
	if part == "*" || part == "?" {
		return nil
	}

	base := part
	if before, after, hasStep := strings.Cut(part, "/"); hasStep {
		if before == "" {
			return errors.New("missing range before step")
		}
		if after == "" {
			return errors.New("missing step")
		}

		step, err := strconv.Atoi(after)
		if err != nil || step <= 0 {
			return fmt.Errorf("invalid step %q", after)
		}

		base = before
		if base == "*" || base == "?" {
			return nil
		}
	}

	return spec.validateRangeOrValue(base)
}

func (spec cronFieldSpec) validateRangeOrValue(token string) error {
	startText, endText, hasRange := strings.Cut(token, "-")
	if hasRange {
		if startText == "" || endText == "" {
			return fmt.Errorf("invalid range %q", token)
		}

		start, err := spec.parseValue(startText)
		if err != nil {
			return err
		}
		end, err := spec.parseValue(endText)
		if err != nil {
			return err
		}
		if start > end {
			return fmt.Errorf("range start %d greater than end %d", start, end)
		}
		return nil
	}

	_, err := spec.parseValue(token)
	return err
}

func (spec cronFieldSpec) parseValue(token string) (int, error) {
	if spec.names != nil {
		if value, ok := spec.names[strings.ToUpper(strings.TrimSpace(token))]; ok {
			return value, nil
		}
	}

	value, err := strconv.Atoi(token)
	if err != nil {
		return 0, fmt.Errorf("invalid value %q", token)
	}
	if value < spec.min || value > spec.max {
		return 0, fmt.Errorf("value %d out of range %d-%d", value, spec.min, spec.max)
	}

	return value, nil
}
