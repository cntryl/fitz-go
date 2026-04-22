// Package kv implements the Fitz KV domain client.
// Per CLIENT_SPEC.md: Transaction-based key-value store with read/write and read-only transactions.
package kv

import (
	"context"
	"fmt"

	"github.com/cntryl/fitz-go/internal/core/connection"
	"github.com/cntryl/fitz-go/internal/core/types"
	"github.com/cntryl/fitz-go/internal/protocol"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Client provides transaction-based key-value operations only. All data
// interactions MUST occur through transactions returned by Begin.
// Convenience helpers were intentionally removed to avoid accidental
// non-transactional use.
type Client interface {
	// Begin opens a transaction scoped to the provided route.
	// Durability is explicit; optional functional options can configure mode.
	Begin(ctx context.Context, route string, durability uint8, opts ...BeginOption) (Tx, error)
}

// BeginOption configures transaction BEGIN parameters.
type BeginOption func(*beginConfig)

// beginConfig holds configuration for BEGIN operations.
type beginConfig struct {
	mode uint8
}

// WithMode sets the transaction mode.
func WithMode(mode uint8) BeginOption {
	return func(cfg *beginConfig) {
		cfg.mode = mode
	}
}

// client is a concrete implementation of Client using the connection layer.
type client struct {
	conn *connection.Connection
}

// NewClient creates a new KV domain client backed by the provided connection.
func NewClient(conn *connection.Connection) Client {
	return &client{
		conn: conn,
	}
}

// Begin opens a transaction scoped to the provided route.
// Per CLIENT_SPEC.md: Server assigns tx_id and returns it in response.
func (c *client) Begin(ctx context.Context, route string, durability uint8, opts ...BeginOption) (Tx, error) {
	ctx, span := c.conn.Tracer().Start(ctx, "fitz.kv.Begin", trace.WithAttributes(attribute.String("fitz.route", route)))
	defer span.End()
	if log := c.conn.Logger(); log != nil {
		log.Debug("kv.Begin", "route", route)
	}

	// Validate route format
	if err := types.ValidateFixedRoute(route, "kv", 3); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("invalid route: %w", err)
	}

	// Apply options
	cfg := beginConfig{
		mode: TxModeReadWrite,
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	// Encode BEGIN request per CLIENT_SPEC.md
	// Send request and wait for response
	resp, err := c.conn.SendRequestWithWriter(ctx, protocol.MessageTypeKvBegin, beginPayloadWriter(route, cfg.mode, durability))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("BEGIN request failed: %w", err)
	}

	// Parse response: [u8 status][u64 BE tx_id] for success
	success, remaining, err := connection.ParseStandardResponse(resp)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("BEGIN failed: %w", mapKVError(err))
	}
	if !success {
		recordErr := fmt.Errorf("BEGIN failed: unexpected status")
		span.RecordError(recordErr)
		span.SetStatus(codes.Error, recordErr.Error())
		return nil, recordErr
	}

	// Extract tx_id from remaining payload (server-assigned per CLIENT_SPEC.md)
	if len(remaining) < 8 { // tx_id is u64 (8 bytes)
		recordErr := fmt.Errorf("invalid BEGIN response: expected at least 8 bytes for tx_id, got %d", len(remaining))
		span.RecordError(recordErr)
		span.SetStatus(codes.Error, recordErr.Error())
		return nil, recordErr
	}

	txID, _, err := connection.ReadU64BE(remaining, 0)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("parse tx_id: %w", err)
	}

	// Create transaction with server-assigned tx_id
	tx := &transaction{
		route:    route,
		conn:     c.conn,
		readOnly: cfg.mode == TxModeReadOnly,
		txID:     txID,
	}

	return tx, nil
}
