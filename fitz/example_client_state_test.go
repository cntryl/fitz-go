package fitz_test

import (
	"fmt"

	"github.com/cntryl/fitz-go/v2/fitz"
)

func ExampleClient_State() {
	var state = fitz.ConnectionStateReconnecting
	fmt.Println(state == fitz.ConnectionStateReconnecting)
	// Output:
	// true
}
