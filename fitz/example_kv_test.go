package fitz_test

import (
	"context"

	"github.com/cntryl/fitz-go/fitz"
)

func ExampleClient_KV() {
	client := fitz.NewClient("localhost:4091", func(context.Context) (string, error) {
		return "", nil
	})
	defer client.Close()

	kv := client.KV()
	_ = kv
}
