package merged_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/lexfrei/extractedprism/internal/discovery/merged"
	"github.com/lexfrei/extractedprism/internal/metrics"
)

func scrapeMergedMetrics(t *testing.T, m *metrics.Metrics) string {
	t.Helper()

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)

	body, err := io.ReadAll(rec.Result().Body)
	require.NoError(t, err)

	return string(body)
}

func TestMerged_RecordsDiscoveryMetrics(t *testing.T) {
	m := metrics.New()

	good := &mockProvider{
		name: "good",
		sendFunc: func(ctx context.Context, ch chan<- []string) error {
			ch <- []string{stableEndpoint}

			<-ctx.Done()

			return nil
		},
	}
	bad := &mockProvider{
		name:     "bad",
		sendFunc: func(_ context.Context, _ chan<- []string) error { return errors.New("boom") },
	}

	mp := merged.NewMergedProvider(zaptest.NewLogger(t), m, good, bad)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	updateCh := make(chan []string, 10)
	done := make(chan error, 1)

	go func() { done <- mp.Run(ctx, updateCh) }()

	select {
	case <-updateCh:
	case <-time.After(5 * time.Second):
		t.Fatal("merged provider did not forward an update")
	}

	body := scrapeMergedMetrics(t, m)
	assert.Contains(t, body, `extractedprism_discovery_updates_total{provider="good"} 1`)

	// The failing provider's goroutine is not guaranteed to have run by the
	// time the good provider's update arrives.
	require.Eventually(t, func() bool {
		return strings.Contains(scrapeMergedMetrics(t, m), `extractedprism_discovery_errors_total{provider="bad"} 1`)
	}, 5*time.Second, 10*time.Millisecond)

	cancel()
	require.NoError(t, <-done, "one failing provider must not fail the merged run")
}

func TestMerged_NilMetrics_PanicsNowhere(t *testing.T) {
	// nil metrics are valid: metrics are optional wiring, and a nil sink
	// must not crash updates or error paths.
	mp := merged.NewMergedProvider(zaptest.NewLogger(t), nil,
		newImmediateProvider([]string{stableEndpoint}),
		newErrorProvider(errors.New("boom")),
	)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	updateCh := make(chan []string, 10)
	done := make(chan error, 1)

	go func() { done <- mp.Run(ctx, updateCh) }()

	select {
	case <-updateCh:
	case <-time.After(5 * time.Second):
		t.Fatal("merged provider did not forward an update")
	}

	cancel()
	require.NoError(t, <-done)
}
