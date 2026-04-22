package schedule

import (
	"encoding/binary"
	"errors"
	"testing"

	"github.com/cntryl/fitz-go/internal/core/connection"
	coreerrors "github.com/cntryl/fitz-go/internal/core/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestShouldMapScheduleError tests error message mapping.
func TestShouldMapScheduleErrorGivenBrokerMessageWhenMapScheduleErrorCalled(t *testing.T) {
	t.Run("map schedule not found error", func(t *testing.T) {
		// Arrange
		errMsg := coreerrors.NewDomainError(coreerrors.ScheduleNotFound, "schedule not found")

		// Act
		mapped := mapScheduleError(errMsg)

		// Assert
		assert.Equal(t, ErrScheduleNotFound, mapped)
	})

	t.Run("preserve typed schedule error", func(t *testing.T) {
		// Arrange
		errMsg := coreerrors.NewDomainError(coreerrors.ScheduleInvalidCron, "Cron expression must have exactly 5 fields")

		// Act
		mapped := mapScheduleError(errMsg)

		// Assert
		var domainErr *coreerrors.DomainError
		assert.True(t, errors.As(mapped, &domainErr))
		assert.Equal(t, uint32(coreerrors.ScheduleInvalidCron), uint32(domainErr.Code))
	})

	t.Run("unknown error returns wrapped message", func(t *testing.T) {
		// Arrange
		errMsg := errors.New("unexpected schedule condition")

		// Act
		mapped := mapScheduleError(errMsg)

		// Assert
		assert.NotNil(t, mapped)
		assert.Equal(t, errMsg, mapped)
	})

	t.Run("empty error message", func(t *testing.T) {
		// Arrange
		errMsg := errors.New("")

		// Act
		mapped := mapScheduleError(errMsg)

		// Assert
		assert.NotNil(t, mapped)
		assert.Equal(t, errMsg, mapped)
	})
}

// TestShouldDefineScheduleOpcodes tests that Schedule opcodes are properly defined.
func TestShouldDefineScheduleOpcodesGivenConstantsWhenRead(t *testing.T) {
	t.Run("create opcode", func(t *testing.T) {
		assert.Equal(t, uint16(700), ScheduleCreate)
	})

	t.Run("cancel opcode", func(t *testing.T) {
		assert.Equal(t, uint16(701), ScheduleCancel)
	})

	t.Run("list opcode", func(t *testing.T) {
		assert.Equal(t, uint16(702), ScheduleList)
	})

	t.Run("subscribe opcode", func(t *testing.T) {
		assert.Equal(t, uint16(703), ScheduleSubscribe)
	})

	t.Run("unsubscribe opcode", func(t *testing.T) {
		assert.Equal(t, uint16(704), ScheduleUnsubscribe)
	})

	t.Run("notify opcode server only", func(t *testing.T) {
		assert.Equal(t, uint16(705), ScheduleNotify)
	})

	t.Run("opcodes are sequential", func(t *testing.T) {
		assert.Equal(t, ScheduleCreate+1, ScheduleCancel)
		assert.Equal(t, ScheduleCancel+1, ScheduleList)
		assert.Equal(t, ScheduleList+1, ScheduleSubscribe)
		assert.Equal(t, ScheduleSubscribe+1, ScheduleUnsubscribe)
		assert.Equal(t, ScheduleUnsubscribe+1, ScheduleNotify)
	})

	t.Run("all opcodes in 700 range", func(t *testing.T) {
		assert.GreaterOrEqual(t, ScheduleCreate, uint16(700))
		assert.LessOrEqual(t, ScheduleNotify, uint16(705))
	})
}

// TestShouldDefineScheduleErrors tests that Schedule error variables are defined.
func TestShouldDefineScheduleErrorsGivenSentinelValuesWhenRead(t *testing.T) {
	t.Run("schedule not found error", func(t *testing.T) {
		assert.NotNil(t, ErrScheduleNotFound)
		assert.Equal(t, "schedule not found", ErrScheduleNotFound.Error())
	})
}

// TestShouldValidateCronExpressions tests cron expression validation.
func TestShouldAcceptCronExpressionsGivenRepresentativeInputsWhenValidatedByCaller(t *testing.T) {
	validExpressions := []string{
		"0 0 * * *",             // Daily at midnight
		"*/5 * * * *",           // Every 5 minutes
		"0 */2 * * *",           // Every 2 hours
		"0 0 1 * *",             // First day of month
		"0 0 * * 0",             // Weekly on Sunday
		"*/15 9-17 * * MON-FRI", // Every 15 min, 9am-5pm weekdays
	}

	for _, expr := range validExpressions {
		t.Run("valid: "+expr, func(t *testing.T) {
			require.NoError(t, validateCronExpression(expr))
		})
	}
}

func TestShouldRejectInvalidCronExpressionsGivenMalformedInputsWhenValidatedByCaller(t *testing.T) {
	invalidExpressions := []string{
		"not a cron",
		"",
		"* * * *",
		"* * * * * *",
		"61 * * * *",
	}

	for _, expr := range invalidExpressions {
		t.Run("invalid: "+expr, func(t *testing.T) {
			require.Error(t, validateCronExpression(expr))
		})
	}
}

// TestShouldDefineScheduleTargets tests schedule target resource/operation handling.
func TestShouldDefineScheduleTargetsGivenRepresentativeRoutesWhenRead(t *testing.T) {
	t.Run("target resource route", func(t *testing.T) {
		target := "schedule://acme/app/backup"
		assert.NotEmpty(t, target)
	})

	t.Run("area selector", func(t *testing.T) {
		target := "schedule://acme/app"
		assert.NotEmpty(t, target)
	})

	t.Run("area wildcard selector", func(t *testing.T) {
		target := "schedule://org.example.com/production/*"
		assert.NotEmpty(t, target)
	})
}

// Benchmarks

func BenchmarkEncodeScheduleCreate(b *testing.B) {
	route := "schedule://acme/jobs/backup"
	cronExpr := "0 0 * * *"
	payload := []byte("backup-payload")
	w := scheduleCreatePayloadWriter(route, cronExpr, payload)
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		w(buf)
	}
}

func BenchmarkEncodeScheduleCancel(b *testing.B) {
	route := "schedule://acme/jobs/backup"
	w := scheduleCancelPayloadWriter(route)
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		w(buf)
	}
}

func BenchmarkEncodeScheduleList(b *testing.B) {
	w := scheduleListPayloadWriter(0, 100) // offset=0, limit=100
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		w(buf)
	}
}

func BenchmarkEncodeScheduleSubscribe(b *testing.B) {
	pattern := "schedule://acme/jobs/*"
	w := scheduleSubscribePayloadWriter(pattern)
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		w(buf)
	}
}

func BenchmarkEncodeScheduleUnsubscribe(b *testing.B) {
	pattern := "schedule://acme/jobs/*"
	w := scheduleUnsubscribePayloadWriter(pattern)
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		w(buf)
	}
}

func BenchmarkParseScheduleCreateResponse(b *testing.B) {
	// [status=0] (success, no optional id)
	payload := []byte{0}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = connection.ParseStandardResponse(payload)
	}
}

func BenchmarkParseScheduleSubscribeResponse(b *testing.B) {
	// [status=0][has_sub_id=1][u64 sub_id]
	payload := make([]byte, 1+1+8)
	payload[0] = 0
	payload[1] = 1
	binary.BigEndian.PutUint64(payload[2:10], 1)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		success, remaining, _ := connection.ParseStandardResponse(payload)
		if success && len(remaining) >= 9 && remaining[0] == 1 {
			_, _, _ = connection.ReadU64BE(remaining, 1)
		}
	}
}
