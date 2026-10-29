//go:build integration

package conformance

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestShouldFailCoverageGivenMissingP0ScenarioWhenAggregated(t *testing.T) {
	collector := &collector{}
	for number := 1; number <= 24; number++ {
		if number == 19 {
			continue
		}
		collector.record(ScenarioResult{ScenarioID: fmt.Sprintf("CS-%03d", number), Verdict: VerdictPass})
	}
	collector.record(ScenarioResult{ScenarioID: "GO-002", Priority: "P0", Verdict: VerdictPass})

	aggregate := collector.aggregate("client", "tcp", "anonymous")

	require.Equal(t, "fail", aggregate.OverallStatus)
	require.Equal(t, []string{"CS-019"}, aggregate.MissingScenarios)
	require.Less(t, aggregate.P0PassRate, 1.0)
}

func TestShouldWarnGivenMissingP1ScenarioWhenAggregated(t *testing.T) {
	collector := &collector{}
	for number := 1; number <= 24; number++ {
		if number == 23 {
			continue
		}
		collector.record(ScenarioResult{ScenarioID: fmt.Sprintf("CS-%03d", number), Verdict: VerdictPass})
	}

	aggregate := collector.aggregate("client", "tcp", "anonymous")

	require.Equal(t, "partial", aggregate.OverallStatus)
	require.Equal(t, []string{"CS-023"}, aggregate.MissingScenarios)
	require.Equal(t, 1.0, aggregate.P0PassRate)
}
