package fitz

import "errors"

// ErrNotImplemented is used by no-op domain clients in the test scaffold.
var ErrNotImplemented = errors.New("not implemented")
