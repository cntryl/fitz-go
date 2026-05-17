//nolint:errcheck
package fitz_test

import (
	"context"

	"github.com/cntryl/fitz-go/fitz"
)

func ExampleClient_RPC() {
	client := fitz.NewClient("localhost:4091", func(context.Context) (string, error) {
		return "", nil
	})
	defer client.Close()

	rpc := client.RPC()
	_ = rpc
}
