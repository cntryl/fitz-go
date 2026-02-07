package lease

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/cntryl/cntryl-go/internal/core/transport"
)

// Domain-specific errors (mapped from broker error responses).
var (
	ErrLeaseHeld     = errors.New("lease held")
	ErrInvalidFence  = errors.New("invalid fencing token")
	ErrLeaseExpired  = errors.New("lease expired")
	ErrLeaseNotFound = errors.New("lease not found")
)

// Wire operation codes for Lease domain. The canonical spec uses 400–403,
// here we store the low-byte values as uint8 for use on the transport frame
// Type field (server and client must agree on the encoding).
const (
	LeaseAcquire uint8 = 400 % 256 // 144
	LeaseRenew   uint8 = 401 % 256 // 145
	LeaseRelease uint8 = 402 % 256 // 146
	LeaseQuery   uint8 = 403 % 256 // 147
)

// LeaseInfo holds the current status of a lease as returned by Query.
type LeaseInfo struct {
	Held      bool
	Token     []byte
	TTL       uint32
	ExpiresAt int64
}

// Client provides ephemeral exclusive lease primitives.
type Client interface {
	Acquire(ctx context.Context, route string, ttlSecs uint32) (token []byte, expiresAt int64, held bool, err error)
	Renew(ctx context.Context, route string, token []byte, ttlSecs uint32) (expiresAt int64, err error)
	Release(ctx context.Context, route string, token []byte) error
	Query(ctx context.Context, route string) (*LeaseInfo, error)
}

// muxProvider is the minimal transport interface needed by the lease client.
// *transport.Mux satisfies this interface.
type muxProvider interface {
	Send(transport.Frame) error
	In() <-chan transport.Frame
}

// client is a concrete implementation of lease.Client backed by the transport mux.
type client struct {
	mux muxProvider
}

// NewClient creates a new lease domain client backed by the transport mux.
func NewClient(mux muxProvider) Client {
	return &client{mux: mux}
}

// recvLeaseFrame waits for the next frame on the lease channel, filtering
// out unrelated frames. Returns an error if the context is cancelled or mux closes.
func (c *client) recvLeaseFrame(ctx context.Context) (transport.Frame, error) {
	for {
		select {
		case <-ctx.Done():
			return transport.Frame{}, ctx.Err()
		case f, ok := <-c.mux.In():
			if !ok {
				return transport.Frame{}, errors.New("mux closed")
			}
			if f.Channel != transport.ChannelLease {
				continue
			}
			return f, nil
		}
	}
}

// mapLeaseError maps a broker error message to a domain-specific Go error
// when the message matches a known pattern.
func mapLeaseError(msg string) error {
	lower := strings.ToLower(msg)
	switch {
	case strings.Contains(lower, "held"):
		return ErrLeaseHeld
	case strings.Contains(lower, "invalid") && (strings.Contains(lower, "fence") || strings.Contains(lower, "token")):
		return ErrInvalidFence
	case strings.Contains(lower, "expired"):
		return ErrLeaseExpired
	case strings.Contains(lower, "not found"):
		return ErrLeaseNotFound
	default:
		return errors.New(msg)
	}
}

// decodeTLVError extracts an error message from a TLV error frame and maps
// it to a domain-specific error when possible.
func decodeTLVError(f transport.Frame, defaultMsg string) error {
	dec, err := transport.NewTLVDecoder(f.Body)
	if err != nil {
		return mapLeaseError(defaultMsg)
	}
	msg := dec.GetString(transport.TagErr)
	if msg == "" {
		msg = defaultMsg
	}
	return mapLeaseError(msg)
}

// Acquire attempts to acquire a lease for the given route and TTL (seconds).
// Returns the opaque lease token, expiry timestamp (Unix seconds), whether the
// lease was granted (held), or an error.
func (c *client) Acquire(ctx context.Context, route string, ttlSecs uint32) (token []byte, expiresAt int64, held bool, err error) {
	enc := transport.NewTLVEncoder()
	enc.AddString(transport.TagRoute, route)
	enc.AddUint32(transport.TagTTL, ttlSecs)
	frame := transport.Frame{
		Type:    LeaseAcquire,
		Flags:   0,
		Channel: transport.ChannelLease,
		Body:    enc.Encode(),
	}
	if err := c.mux.Send(frame); err != nil {
		return nil, 0, false, fmt.Errorf("send failed: %w", err)
	}

	for {
		f, err := c.recvLeaseFrame(ctx)
		if err != nil {
			return nil, 0, false, err
		}
		switch f.Type {
		case transport.FrameTypeResp:
			dec, derr := transport.NewTLVDecoder(f.Body)
			if derr != nil {
				return nil, 0, false, fmt.Errorf("invalid TLV in response: %w", derr)
			}
			leaseToken := dec.GetBytes(transport.TagLease)
			ttl, _ := dec.GetUint32(transport.TagTTL)
			if len(leaseToken) == 0 {
				// No token -> not held
				return nil, 0, false, nil
			}
			expires := time.Now().Add(time.Duration(ttl) * time.Second).Unix()
			return leaseToken, expires, true, nil
		case transport.FrameTypeErr:
			return nil, 0, false, decodeTLVError(f, "lease acquire failed")
		default:
			continue
		}
	}
}

// Renew extends the lease TTL for an existing lease token.
func (c *client) Renew(ctx context.Context, route string, token []byte, ttlSecs uint32) (expiresAt int64, err error) {
	enc := transport.NewTLVEncoder()
	enc.AddString(transport.TagRoute, route)
	enc.AddBytes(transport.TagLease, token)
	enc.AddUint32(transport.TagTTL, ttlSecs)
	frame := transport.Frame{
		Type:    LeaseRenew,
		Flags:   0,
		Channel: transport.ChannelLease,
		Body:    enc.Encode(),
	}
	if err := c.mux.Send(frame); err != nil {
		return 0, fmt.Errorf("send failed: %w", err)
	}

	for {
		f, err := c.recvLeaseFrame(ctx)
		if err != nil {
			return 0, err
		}
		switch f.Type {
		case transport.FrameTypeResp:
			dec, derr := transport.NewTLVDecoder(f.Body)
			if derr != nil {
				return 0, fmt.Errorf("invalid TLV in response: %w", derr)
			}
			ttl, _ := dec.GetUint32(transport.TagTTL)
			expires := time.Now().Add(time.Duration(ttl) * time.Second).Unix()
			return expires, nil
		case transport.FrameTypeErr:
			return 0, decodeTLVError(f, "lease renew failed")
		default:
			continue
		}
	}
}

// Release frees a lease indicated by token.
func (c *client) Release(ctx context.Context, route string, token []byte) error {
	enc := transport.NewTLVEncoder()
	enc.AddString(transport.TagRoute, route)
	enc.AddBytes(transport.TagLease, token)
	frame := transport.Frame{
		Type:    LeaseRelease,
		Flags:   0,
		Channel: transport.ChannelLease,
		Body:    enc.Encode(),
	}
	if err := c.mux.Send(frame); err != nil {
		return fmt.Errorf("send failed: %w", err)
	}

	for {
		f, err := c.recvLeaseFrame(ctx)
		if err != nil {
			return err
		}
		switch f.Type {
		case transport.FrameTypeResp:
			return nil
		case transport.FrameTypeErr:
			return decodeTLVError(f, "lease release failed")
		default:
			continue
		}
	}
}

// Query retrieves the current status of a lease for the given route.
func (c *client) Query(ctx context.Context, route string) (*LeaseInfo, error) {
	enc := transport.NewTLVEncoder()
	enc.AddString(transport.TagRoute, route)
	frame := transport.Frame{
		Type:    LeaseQuery,
		Flags:   0,
		Channel: transport.ChannelLease,
		Body:    enc.Encode(),
	}
	if err := c.mux.Send(frame); err != nil {
		return nil, fmt.Errorf("send failed: %w", err)
	}

	for {
		f, err := c.recvLeaseFrame(ctx)
		if err != nil {
			return nil, err
		}
		switch f.Type {
		case transport.FrameTypeResp:
			dec, derr := transport.NewTLVDecoder(f.Body)
			if derr != nil {
				return nil, fmt.Errorf("invalid TLV in response: %w", derr)
			}
			leaseToken := dec.GetBytes(transport.TagLease)
			ttl, _ := dec.GetUint32(transport.TagTTL)
			info := &LeaseInfo{
				Held:  len(leaseToken) > 0,
				Token: leaseToken,
				TTL:   ttl,
			}
			if info.Held {
				info.ExpiresAt = time.Now().Add(time.Duration(ttl) * time.Second).Unix()
			}
			return info, nil
		case transport.FrameTypeErr:
			return nil, decodeTLVError(f, "lease query failed")
		default:
			continue
		}
	}
}
