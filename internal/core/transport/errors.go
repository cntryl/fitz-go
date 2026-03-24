package transport

import "fmt"

// TransportError represents a transport-layer failure (dial, handshake, read, write).
// It is distinct from [internal/core/errors.DomainError] which represents
// server-returned domain errors.  Callers can use errors.As to differentiate:
//
//	var te *transport.TransportError
//	if errors.As(err, &te) {
//	    log.Printf("transport failure op=%s: %v", te.Op, te.Cause)
//	}
type TransportError struct {
	Op    string // high-level operation: "dial", "handshake", "read", "write"
	Cause error  // underlying error
}

func (e *TransportError) Error() string {
	return fmt.Sprintf("fitz transport %s: %v", e.Op, e.Cause)
}

func (e *TransportError) Unwrap() error {
	return e.Cause
}

// newTransportError wraps err in a TransportError for the given op.
func newTransportError(op string, err error) error {
	return &TransportError{Op: op, Cause: err}
}
