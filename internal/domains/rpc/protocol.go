package rpc

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/cntryl/fitz-go/internal/core/encoding"
	coreerrors "github.com/cntryl/fitz-go/internal/core/errors"
)

// Wire opcodes for RPC domain (per CLIENT_SPEC.md). Values are message type identifiers.
const (
	RPCSubscribeWorker   uint16 = 300
	RPCUnsubscribeWorker uint16 = 301
	RPCRequest           uint16 = 302
	RPCResponse          uint16 = 303
	RPCCancellation      uint16 = 304
	RPCLifecycle         uint16 = 305
)

// Domain-specific errors. Returned when Call fails or the server rejects a request.
//   - ErrNoWorkers: no worker is registered for the route (or route not registered).
//   - ErrRPCTimeout: the request timed out before receiving a response.
//   - ErrRPCBackpressure: server backpressure.
var (
	ErrNoWorkers       = errors.New("no workers available")
	ErrRPCTimeout      = errors.New("rpc timeout")
	ErrRPCBackpressure = errors.New("rpc backpressure")
)

// mapRPCError maps a broker error to a domain-specific Go error.
func mapRPCError(err error) error {
	if err == nil {
		return nil
	}

	var domainErr *coreerrors.DomainError
	if errors.As(err, &domainErr) {
		switch uint32(domainErr.Code) {
		case coreerrors.RpcTimeout:
			return fmt.Errorf("%w: %w", ErrRPCTimeout, err)
		case coreerrors.RpcWorkerNotFound, coreerrors.RpcRouteNotRegistered:
			return fmt.Errorf("%w: %w", ErrNoWorkers, err)
		case coreerrors.RpcBackpressure:
			return fmt.Errorf("%w: %w", ErrRPCBackpressure, err)
		default:
			return err
		}
	}
	return err
}

// encodeRPCSubscribeWorker encodes an RPC SUBSCRIBE_WORKER request per CLIENT_SPEC.md.
// Wire format: [string worker_route][u32 max_concurrent]
func encodeRPCSubscribeWorker(workerRoute string, maxConcurrent uint32) ([]byte, error) {
	return encoding.EncodeWithBuffer(func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, workerRoute)
		encoding.WriteU32(buf, maxConcurrent)
	}), nil
}

// encodeRPCUnsubscribeWorker encodes an RPC UNSUBSCRIBE_WORKER request per CLIENT_SPEC.md.
// Wire format: [string worker_route]
func encodeRPCUnsubscribeWorker(workerRoute string) ([]byte, error) {
	return encoding.EncodeWithBuffer(func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, workerRoute)
	}), nil
}

// encodeRPCRequest encodes an RPC REQUEST per CLIENT_SPEC.md.
// Wire format: [uuid16 correlation_id][string route][bytes body]
func encodeRPCRequest(correlationID [16]byte, route string, replyRoute string, body []byte) ([]byte, error) {
	return encoding.EncodeWithBuffer(func(buf *bytes.Buffer) {
		buf.Write(correlationID[:])
		encoding.WriteRoute(buf, route)
		encoding.WriteBytes(buf, body)
	}), nil
}

// encodeRPCResponse encodes an RPC RESPONSE per CLIENT_SPEC.md.
// Wire format: [uuid16 correlation_id][u64 sequence][u8 flags][bytes body]
func encodeRPCResponse(correlationID [16]byte, sequence uint64, body []byte, streamEnd bool) ([]byte, error) {
	return encoding.EncodeWithBuffer(func(buf *bytes.Buffer) {
		buf.Write(correlationID[:])
		encoding.WriteU64(buf, sequence)
		if streamEnd {
			buf.WriteByte(1)
		} else {
			buf.WriteByte(0)
		}
		encoding.WriteBytes(buf, body)
	}), nil
}

// Payload writer helpers for zero-copy frame encoding

func rpcSubscribeWorkerPayloadWriter(workerRoute string, maxConcurrent uint32) func(*bytes.Buffer) {
	return rpcSubscribeWorkerPayloadWriterWithCancellation(workerRoute, maxConcurrent, false)
}

func rpcSubscribeWorkerPayloadWriterWithCancellation(workerRoute string, maxConcurrent uint32, supportsCancellation bool) func(*bytes.Buffer) {
	return func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, workerRoute)
		encoding.WriteU32(buf, maxConcurrent)
		var flags byte
		if supportsCancellation {
			flags = 1
		}
		buf.WriteByte(flags)
	}
}

func rpcUnsubscribeWorkerPayloadWriter(workerRoute string) func(*bytes.Buffer) {
	return func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, workerRoute)
	}
}

func rpcRequestPayloadWriterWithBudget(correlationID [16]byte, route string, replyRoute string, body []byte, remainingBudgetMS *uint32) func(*bytes.Buffer) {
	return func(buf *bytes.Buffer) {
		buf.Write(correlationID[:])
		encoding.WriteRoute(buf, route)
		encoding.WriteBytes(buf, body)
		if remainingBudgetMS != nil {
			buf.WriteByte(1)
			encoding.WriteU32(buf, *remainingBudgetMS)
		} else {
			buf.WriteByte(0)
		}
	}
}

func rpcCallerCancellationPayloadWriter(correlationID [16]byte, reason byte) func(*bytes.Buffer) {
	return func(buf *bytes.Buffer) {
		buf.WriteByte(1)
		buf.Write(correlationID[:])
		buf.WriteByte(reason)
	}
}

func rpcWorkerCleanupAckPayloadWriter(correlationID [16]byte) func(*bytes.Buffer) {
	return func(buf *bytes.Buffer) {
		buf.WriteByte(3)
		buf.Write(correlationID[:])
	}
}

func rpcResponsePayloadWriter(correlationID [16]byte, sequence uint64, body []byte, streamEnd bool) func(*bytes.Buffer) {
	return func(buf *bytes.Buffer) {
		buf.Write(correlationID[:])
		encoding.WriteU64(buf, sequence)
		if streamEnd {
			buf.WriteByte(1)
		} else {
			buf.WriteByte(0)
		}
		encoding.WriteBytes(buf, body)
	}
}
