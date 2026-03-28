package fitz_test

import (
	"context"

	"github.com/cntryl/fitz-go/fitz"
)

func ExampleClient_Notice() {
	client := fitz.NewClient("localhost:4091", func(context.Context) (string, error) {
		return "", nil
	})
	defer client.Close()

	notice := client.Notice()
	_ = notice
}

func ExampleClient_Queue() {
	client := fitz.NewClient("localhost:4091", func(context.Context) (string, error) {
		return "", nil
	})
	defer client.Close()

	queue := client.Queue()
	_ = queue
}

func ExampleClient_Lease() {
	client := fitz.NewClient("localhost:4091", func(context.Context) (string, error) {
		return "", nil
	})
	defer client.Close()

	lease := client.Lease()
	_ = lease
}

func ExampleClient_Stream() {
	client := fitz.NewClient("localhost:4091", func(context.Context) (string, error) {
		return "", nil
	})
	defer client.Close()

	stream := client.Stream()
	_ = stream
}

func ExampleClient_Schedule() {
	client := fitz.NewClient("localhost:4091", func(context.Context) (string, error) {
		return "", nil
	})
	defer client.Close()

	schedule := client.Schedule()
	_ = schedule
}
