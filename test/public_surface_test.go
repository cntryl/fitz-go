//go:build integration

package integration

import (
	"reflect"
	"testing"

	"github.com/cntryl/fitz-go/v2/fitz"
)

func TestPublicSurface(t *testing.T) {
	t.Run("client surface omits raw request/send", func(t *testing.T) {
		assertNoMethod(t, reflect.TypeOf(&fitz.Client{}), "RequestAsync")
		assertNoMethod(t, reflect.TypeOf(&fitz.Client{}), "SendAsync")
	})

	t.Run("rpc request hides correlation id", func(t *testing.T) {
		assertPublicFields(t, reflect.TypeOf(fitz.RPCInboundRequest{}), []string{"Route", "ReplyRoute", "Body"})
		assertUnexportedField(t, reflect.TypeOf(fitz.RPCInboundRequest{}), "correlationID")
	})

	t.Run("queue item hides reservation ids", func(t *testing.T) {
		assertPublicFields(t, reflect.TypeOf(fitz.QueueItem{}), []string{"Body", "Route"})
		assertUnexportedField(t, reflect.TypeOf(fitz.QueueItem{}), "id")
		assertUnexportedField(t, reflect.TypeOf(fitz.QueueItem{}), "token")
	})

	t.Run("lease hides fencing token", func(t *testing.T) {
		assertPublicFields(t, reflect.TypeOf(fitz.Lease{}), []string{"ExpiresAt"})
		assertUnexportedField(t, reflect.TypeOf(fitz.Lease{}), "token")
	})

	t.Run("lease info remains token-free", func(t *testing.T) {
		assertNoField(t, reflect.TypeOf(fitz.LeaseInfo{}), "Token")
	})
}

func assertNoMethod(t *testing.T, typ reflect.Type, name string) {
	t.Helper()

	if _, ok := typ.MethodByName(name); ok {
		t.Fatalf("unexpected public method %s on %s", name, typ)
	}
}

func assertPublicFields(t *testing.T, typ reflect.Type, expected []string) {
	t.Helper()

	fields := make([]string, 0, typ.NumField())
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if field.PkgPath == "" {
			fields = append(fields, field.Name)
		}
	}

	if !reflect.DeepEqual(fields, expected) {
		t.Fatalf("unexpected public fields for %s: got %v want %v", typ, fields, expected)
	}
}

func assertUnexportedField(t *testing.T, typ reflect.Type, name string) {
	t.Helper()

	field, ok := typ.FieldByName(name)
	if !ok {
		t.Fatalf("expected field %s on %s", name, typ)
	}

	if field.PkgPath == "" {
		t.Fatalf("field %s on %s is unexpectedly exported", name, typ)
	}
}

func assertNoField(t *testing.T, typ reflect.Type, name string) {
	t.Helper()

	if _, ok := typ.FieldByName(name); ok {
		t.Fatalf("unexpected field %s on %s", name, typ)
	}
}
