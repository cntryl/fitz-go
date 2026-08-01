package kv

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/cntryl/fitz-go/internal/core/connection"
	coretypes "github.com/cntryl/fitz-go/internal/core/types"
	"github.com/cntryl/fitz-go/internal/testkit"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShouldDeliverConcreteRouteGivenWildcardKVSubscriptionWhenNotificationArrives(t *testing.T) {
	// Arrange
	transport := testkit.NewMockTransport()
	conn := connection.New(transport, connection.DefaultConfig())
	require.NoError(t, conn.Start(context.Background()))
	defer func() { require.NoError(t, conn.Close()) }()
	client := NewClient(conn).(*client)
	received := make(chan ChangeNotification, 1)
	_, _, err := client.subscriptions.Subscribe("kv://*/area/**", func(_ context.Context, notification ChangeNotification) error {
		received <- notification
		return nil
	}, func(string) (uint64, error) {
		return 42, nil
	})
	require.NoError(t, err)
	mutationCount := make([]byte, 8)
	binary.BigEndian.PutUint64(mutationCount, 3)

	// Act
	client.handleNotify(42, "kv://realm/area/resource", mutationCount)

	// Assert
	select {
	case notification := <-received:
		assert.Equal(t, "kv://realm/area/resource", notification.Route)
		assert.Equal(t, uint64(3), notification.MutationCount)
	case <-time.After(time.Second):
		t.Fatal("KV change notification not delivered")
	}
}

func TestShouldRejectInvalidKVRegistrationPatternsBeforeSending(t *testing.T) {
	// Arrange
	patterns := []string{
		"queue://realm/area/resource",
		"kv://realm//resource",
		"kv://realm/area/res*",
		"kv://realm/area",
		"kv://realm/area/resource/extra/**",
	}
	results := make([]error, 0, len(patterns))

	// Act
	for _, pattern := range patterns {
		results = append(results, coretypes.ValidateRegistrationPattern(pattern, "kv", 3))
	}

	// Assert
	for index, result := range results {
		assert.Error(t, result, patterns[index])
	}
}
