package integration

import (
	"testing"
)

// TestShouldReceiveNotificationGivenActiveSubscriptionWhenPublishMatches
// verifies the basic pub/sub lifecycle: SUBSCRIBE → PUBLISH → NOTIFY.
func TestShouldReceiveNotificationGivenActiveSubscriptionWhenPublishMatches(t *testing.T) {
	t.Skip("TODO: implement notice integration tests against current Client interface")
}

// TestShouldFanoutToAllSubscribersGivenMultipleSubscriptionsWhenPublish
// verifies a single PUBLISH reaches all matching subscriptions.
func TestShouldFanoutToAllSubscribersGivenMultipleSubscriptionsWhenPublish(t *testing.T) {
	t.Skip("TODO: implement notice integration tests against current Client interface")
}

// TestShouldStopReceivingGivenUnsubscribeWhenPublish
// verifies unsubscribe removes subscription from broker.
func TestShouldStopReceivingGivenUnsubscribeWhenPublish(t *testing.T) {
	t.Skip("TODO: implement notice integration tests against current Client interface")
}
