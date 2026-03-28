package fitz_test

import (
	"fmt"

	"github.com/cntryl/fitz-go/fitz"
)

func ExampleClient_State() {
	var state fitz.ConnectionState = fitz.ConnectionStateReconnecting
	fmt.Println(state == fitz.ConnectionStateReconnecting)
	// Output:
	// true
}
