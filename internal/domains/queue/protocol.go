package queue

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cntryl/fitz-go/v2/internal/core/connection"
	"github.com/cntryl/fitz-go/v2/internal/core/encoding"
)

// Wire opcodes for Queue domain (per CLIENT_SPEC.md). Values are message type identifiers.
const (
	QueueEnqueue  uint16 = 200
	QueueReserve  uint16 = 202
	QueueExtend   uint16 = 203
	QueueComplete uint16 = 204
)

// Domain-specific errors. Returned when the server rejects a queue operation.
//   - ErrInvalidToken: complete or extend used an invalid or wrong lease token.
//   - ErrLeaseExpiredQ: lease on a reserved message has expired (Q suffix to avoid clash with lease package).
//   - ErrMessageNotFound: the message id or token is unknown.
//   - ErrQueueNotFound: the queue route does not exist.
//   - ErrQueueFull: backpressure.
var (
	ErrInvalidToken    = errors.New("invalid token")
	ErrLeaseExpiredQ   = errors.New("lease expired")
	ErrMessageNotFound = errors.New("message not found")
	ErrQueueNotFound   = errors.New("queue not found")
	ErrQueueFull       = errors.New("queue full")
)

// Queue error codes in the shared domain error envelope:
// [u8 status=1][u32 code][u32 msg_len][msg].
const (
	queueDomainErrInvalidToken  = 4001
	queueDomainErrLeaseExpired  = 4002
	queueDomainErrNotFound      = 4003
	queueDomainErrQueueNotFound = 4004
	queueDomainErrQueueFull     = 4005
)

// parseQueueResponse maps the Queue domain's shared error envelope to public errors.
func parseQueueResponse(payload []byte) (bool, []byte, error) {
	if len(payload) < 1 {
		return false, nil, errors.New("response too short")
	}

	status := payload[0]
	if status == 0 {
		return true, payload[1:], nil
	}

	if len(payload) < 9 {
		return false, nil, fmt.Errorf("malformed queue error response (%d bytes)", len(payload))
	}

	code := binary.BigEndian.Uint32(payload[1:5])
	msgLen := binary.BigEndian.Uint32(payload[5:9])
	if uint64(msgLen) != uint64(len(payload)-9) {
		return false, nil, fmt.Errorf("malformed queue error response (%d bytes)", len(payload))
	}
	return false, nil, mapQueueErrorCode(code, string(payload[9:]))
}

func parsePlainQueueResponse(payload []byte) (bool, []byte, error) {
	if len(payload) < 1 {
		return false, nil, errors.New("response too short")
	}
	if payload[0] == 0 {
		return true, payload[1:], nil
	}
	message, end, err := connection.ReadString(payload, 1)
	if err != nil || end != len(payload) {
		return false, nil, fmt.Errorf("malformed plain queue error response (%d bytes)", len(payload))
	}
	return false, nil, mapPlainQueueError(message)
}

func mapPlainQueueError(message string) error {
	switch message {
	case "InvalidToken":
		return ErrInvalidToken
	case "InflightExpired":
		return ErrLeaseExpiredQ
	case "NotFound":
		return ErrMessageNotFound
	case "QueueNotFound":
		return ErrQueueNotFound
	default:
		return errors.New(message)
	}
}

func mapQueueErrorCode(code uint32, msg string) error {
	switch code {
	case queueDomainErrInvalidToken:
		return ErrInvalidToken
	case queueDomainErrLeaseExpired:
		return ErrLeaseExpiredQ
	case queueDomainErrNotFound:
		return ErrMessageNotFound
	case queueDomainErrQueueNotFound:
		return ErrQueueNotFound
	case queueDomainErrQueueFull:
		return ErrQueueFull
	default:
		return fmt.Errorf("queue error %d: %s", code, msg)
	}
}

// encodeEnqueue encodes a Queue ENQUEUE request payload per CLIENT_SPEC.md.
// Spec: [route_len][route][body_len][body][has_delay][delay_seconds if has_delay]
func encodeEnqueue(route string, body []byte, delaySeconds uint64) []byte {
	routeBytes := []byte(route)
	hasDelay := uint8(0)
	if delaySeconds > 0 {
		hasDelay = 1
	}

	payloadSize := 4 + len(routeBytes) + 4 + len(body) + 1
	if hasDelay == 1 {
		payloadSize += 8
	}
	payload := make([]byte, 0, payloadSize)

	// [u32 BE] route_len
	routeLenBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(routeLenBytes, uint32(len(routeBytes)))
	payload = append(payload, routeLenBytes...)

	// [bytes] route
	payload = append(payload, routeBytes...)

	// [u32 BE] body_len
	bodyLenBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(bodyLenBytes, uint32(len(body)))
	payload = append(payload, bodyLenBytes...)

	// [bytes] body
	payload = append(payload, body...)

	// [u8] has_delay
	payload = append(payload, hasDelay)

	// [u64 BE] delay_seconds (if has_delay)
	if hasDelay == 1 {
		delayBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(delayBytes, delaySeconds)
		payload = append(payload, delayBytes...)
	}

	return payload
}

// encodeReserve encodes a Queue RESERVE request payload per CLIENT_SPEC.md.
// Spec: [route_len][route][lease_seconds][has_batch_size][batch_size if present]
func encodeReserve(route string, leaseSeconds uint64, batchSize uint32) []byte {
	routeBytes := []byte(route)
	hasBatchSize := uint8(0)
	if batchSize > 0 {
		hasBatchSize = 1
	}

	payloadSize := 4 + len(routeBytes) + 8 + 1
	if hasBatchSize == 1 {
		payloadSize += 4
	}
	payload := make([]byte, 0, payloadSize)

	// [u32 BE] route_len
	routeLenBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(routeLenBytes, uint32(len(routeBytes)))
	payload = append(payload, routeLenBytes...)

	// [bytes] route
	payload = append(payload, routeBytes...)

	// [u64 BE] lease_seconds
	leaseBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(leaseBytes, leaseSeconds)
	payload = append(payload, leaseBytes...)

	// [u8] has_batch_size
	payload = append(payload, hasBatchSize)

	// [u32 BE] batch_size (if has_batch_size)
	if hasBatchSize == 1 {
		batchBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(batchBytes, batchSize)
		payload = append(payload, batchBytes...)
	}

	return payload
}

// encodeExtend encodes a Queue EXTEND request payload per CLIENT_SPEC.md.
// Spec: [route_len][route][message_id][lease_token][lease_seconds]
func encodeExtend(route string, messageID uint64, leaseToken uint64, leaseSeconds uint64) ([]byte, error) {
	return encoding.EncodeWithBuffer(func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, route)
		encoding.WriteU64(buf, messageID)
		encoding.WriteU64(buf, leaseToken)
		encoding.WriteU64(buf, leaseSeconds)
	}), nil
}

// encodeComplete encodes a Queue COMPLETE request payload per CLIENT_SPEC.md.
// Spec: [route_len][route][message_id][lease_token]
func encodeComplete(route string, messageID uint64, leaseToken uint64) ([]byte, error) {
	return encoding.EncodeWithBuffer(func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, route)
		encoding.WriteU64(buf, messageID)
		encoding.WriteU64(buf, leaseToken)
	}), nil
}

// Payload writer helpers for zero-copy frame encoding

func enqueuePayloadWriter(route string, body []byte, delaySeconds uint64) func(*bytes.Buffer) {
	return func(buf *bytes.Buffer) {
		routeBytes := []byte(route)
		hasDelay := uint8(0)
		if delaySeconds > 0 {
			hasDelay = 1
		}
		// [u32 BE] route_len
		encoding.WriteU32(buf, uint32(len(routeBytes)))
		// [bytes] route
		buf.Write(routeBytes)
		// [u32 BE] body_len
		encoding.WriteU32(buf, uint32(len(body)))
		// [bytes] body
		buf.Write(body)
		// [u8] has_delay
		buf.WriteByte(hasDelay)
		// [u64 BE] delay_seconds (if has_delay)
		if hasDelay == 1 {
			encoding.WriteU64(buf, delaySeconds)
		}
	}
}

func reservePayloadWriter(route string, leaseSeconds uint64, batchSize uint32, waitSeconds uint64) func(*bytes.Buffer) {
	return func(buf *bytes.Buffer) {
		routeBytes := []byte(route)
		hasBatchSize := uint8(0)
		if batchSize > 0 {
			hasBatchSize = 1
		}
		// [u32 BE] route_len
		encoding.WriteU32(buf, uint32(len(routeBytes)))
		// [bytes] route
		buf.Write(routeBytes)
		// [u64 BE] lease_seconds
		encoding.WriteU64(buf, leaseSeconds)
		// [u8] has_batch_size
		buf.WriteByte(hasBatchSize)
		// [u32 BE] batch_size (if has_batch_size)
		if hasBatchSize == 1 {
			encoding.WriteU32(buf, batchSize)
		}
		if waitSeconds == 0 {
			buf.WriteByte(0)
		} else {
			buf.WriteByte(1)
			encoding.WriteU64(buf, waitSeconds)
		}
	}
}

func extendPayloadWriter(route string, messageID uint64, leaseToken uint64, leaseSeconds uint64) func(*bytes.Buffer) {
	return func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, route)
		encoding.WriteU64(buf, messageID)
		encoding.WriteU64(buf, leaseToken)
		encoding.WriteU64(buf, leaseSeconds)
	}
}

func completePayloadWriter(route string, messageID uint64, leaseToken uint64) func(*bytes.Buffer) {
	return func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, route)
		encoding.WriteU64(buf, messageID)
		encoding.WriteU64(buf, leaseToken)
	}
}
func subscribePayloadWriter(pattern string) func(*bytes.Buffer) {
	return func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, pattern)
	}
}

func unsubscribePayloadWriter(pattern string) func(*bytes.Buffer) {
	return func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, pattern)
	}
}
