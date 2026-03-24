package fitz_test

import (
	"errors"
	"fmt"

	"github.com/cntryl/fitz-go/fitz"
)

func ExampleIsRetryable() {
	err := &fitz.DomainError{Code: 6001, Message: "rpc timeout"}
	fmt.Println(fitz.IsRetryable(err))
	fmt.Println(fitz.IsRetryable(errors.New("invalid route")))
	// Output:
	// true
	// false
}

func ExampleTransportError() {
	wrapped := fmt.Errorf("request failed: %w", &fitz.TransportError{Op: "dial", Cause: errors.New("connection refused")})

	var te *fitz.TransportError
	if errors.As(wrapped, &te) {
		fmt.Println(te.Op)
	}
	// Output:
	// dial
}
