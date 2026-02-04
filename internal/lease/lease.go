package lease

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cntryl/cntryl-go/internal/transport"
)

// Client provides ephemeral exclusive lease primitives.
type Client interface {
	Acquire(ctx context.Context, route string, ttlSecs uint32) (token []byte, expiresAt int64, held bool, err error)
	Renew(ctx context.Context, route string, token []byte, ttlSecs uint32) (expiresAt int64, err error)
	Release(ctx context.Context, route string, token []byte) error
}

// client is a concrete implementation of lease.Client backed by the transport mux.
type client struct {
	mux *transport.Mux
}

// NewClient creates a new lease domain client backed by the transport mux.
func NewClient(mux *transport.Mux) Client {
	return &client{mux: mux}
}

// recvLeaseFrame waits for the next frame on the lease channel, filtering
// out unrelated frames. Returns an error if the context is cancelled.
func (c *client) recvLeaseFrame(ctx context.Context) (transport.Frame, error) {
	for {
		select {
		case <-ctx.Done():
			return transport.Frame{}, ctx.Err()
		case f := <-c.mux.In():
			if f.Channel != transport.ChannelLease {
				continue
			}
			return f, nil
		}
	}
}

// decodeTLVError extracts an error message from a TLV error frame, falling
// back to the provided default if none is present.
func decodeTLVError(f transport.Frame, defaultMsg string) error {
	dec, _ := transport.NewTLVDecoder(f.Body)
	msg := dec.GetString(transport.TagErr)
	if msg == "" {
		msg = defaultMsg
	}
	return errors.New(msg)
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
