package schedule

import (
	"bytes"
	"errors"
	"fmt"
	"strings"

	"github.com/cntryl/fitz-go/v2/internal/core/encoding"
	coreerrors "github.com/cntryl/fitz-go/v2/internal/core/errors"
)

// Wire opcodes for Schedule domain (per CLIENT_SPEC.md). Values are message type identifiers.
const (
	ScheduleCreate       uint16 = 700
	ScheduleCancel       uint16 = 701
	ScheduleListPageType uint16 = 707
	ScheduleSubscribe    uint16 = 703
	ScheduleUnsubscribe  uint16 = 704
	ScheduleNotify       uint16 = 705 // Server -> Client only
)

// Domain-specific errors. Returned when the server rejects a schedule operation.
//   - ErrScheduleNotFound: cancel or list referenced a schedule that does not exist.
var (
	ErrScheduleNotFound            = errors.New("schedule not found")
	ErrScheduleInvalidDeliveryMode = errors.New("invalid schedule delivery mode")
)

// mapScheduleError maps a broker error to a domain-specific Go error.
func mapScheduleError(err error) error {
	if err == nil {
		return nil
	}

	var domainErr *coreerrors.DomainError
	if errors.As(err, &domainErr) {
		switch uint32(domainErr.Code) {
		case coreerrors.ScheduleNotFound:
			return ErrScheduleNotFound
		case coreerrors.ScheduleInvalidDeliveryMode:
			return fmt.Errorf("%w: %s", ErrScheduleInvalidDeliveryMode, domainErr.Message)
		default:
			return err
		}
	}

	l := strings.ToLower(err.Error())
	switch {
	case strings.Contains(l, "not found"):
		return ErrScheduleNotFound
	default:
		return err
	}
}

// Payload writer helpers per CLIENT_SPEC.md (flat concatenation, no nested TLV).

// scheduleCreatePayloadWriter encodes CREATE request: [route_len][route][cron_len][cron][delivery_mode][payload_len][payload]
func scheduleCreatePayloadWriter(route string, cronExpr string, deliveryMode ScheduleDeliveryMode, payload []byte) func(*bytes.Buffer) {
	return func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, route)
		encoding.WriteString(buf, cronExpr)
		buf.WriteByte(byte(deliveryMode))
		encoding.WriteBytes(buf, payload)
	}
}

// scheduleCancelPayloadWriter encodes CANCEL request: [route_len][route] (route-based identity per spec)
func scheduleCancelPayloadWriter(route string) func(*bytes.Buffer) {
	return func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, route)
	}
}

func scheduleListPagePayloadWriter(cursor *string, limit *uint64) func(*bytes.Buffer) {
	return func(buf *bytes.Buffer) {
		if cursor == nil {
			buf.WriteByte(0)
		} else {
			buf.WriteByte(1)
			encoding.WriteString(buf, *cursor)
		}
		if limit == nil {
			buf.WriteByte(0)
		} else {
			encoding.WriteOptionalU64(buf, *limit)
		}
	}
}

func scheduleSubscribePayloadWriter(pattern string) func(*bytes.Buffer) {
	return func(buf *bytes.Buffer) {
		encoding.WriteString(buf, pattern)
	}
}

func scheduleUnsubscribePayloadWriter(pattern string) func(*bytes.Buffer) {
	return func(buf *bytes.Buffer) {
		encoding.WriteString(buf, pattern)
	}
}
