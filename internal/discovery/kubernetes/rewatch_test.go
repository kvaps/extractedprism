package kubernetes_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	kubediscovery "github.com/lexfrei/extractedprism/internal/discovery/kubernetes"
)

// rewatchHarness runs a provider whose clock and backoff are controlled by
// the test. Every Watch call gets a fresh fake watcher, delivered on watchers.
// Every backoff wait hands its attempt number to the test and blocks until
// the test takes it, so the next Watch call can never overtake it.
type rewatchHarness struct {
	watchers chan *watch.FakeWatcher
	backoffs chan int
	resume   chan struct{}
	done     chan struct{}
	elapsed  atomic.Int64
	reported atomic.Int32
	// noBookmarks is set when any Watch call does not ask for bookmarks.
	noBookmarks atomic.Bool
	cancel      context.CancelFunc
	errCh       chan error
	logs        *observer.ObservedLogs
}

// announcingWatcher hands its fake to the test on the first ResultChan call,
// not when Watch returns, so the test cannot age a stream before the provider
// has started timing it.
type announcingWatcher struct {
	*watch.FakeWatcher

	once     sync.Once
	announce func()
}

func (w *announcingWatcher) ResultChan() <-chan watch.Event {
	w.once.Do(w.announce)

	return w.FakeWatcher.ResultChan()
}

// startRewatchHarness fails the first Watch calls, one per failedWatchCalls
// entry, each after moving the clock by that entry while the call is in flight.
func startRewatchHarness(t *testing.T, failedWatchCalls ...time.Duration) *rewatchHarness {
	t.Helper()

	harness := &rewatchHarness{
		watchers: make(chan *watch.FakeWatcher, 10),
		backoffs: make(chan int),
		resume:   make(chan struct{}),
		done:     make(chan struct{}),
		errCh:    make(chan error, 1),
	}

	client := fake.NewClientset(makeEndpointSlice("10.0.0.1"))

	var watchCalls atomic.Int32

	client.PrependWatchReactor("endpointslices",
		func(action k8stesting.Action) (bool, watch.Interface, error) {
			withOptions, ok := action.(interface{ GetListOptions() metav1.ListOptions })
			if !ok || !withOptions.GetListOptions().AllowWatchBookmarks {
				harness.noBookmarks.Store(true)
			}

			call := int(watchCalls.Add(1))
			if call <= len(failedWatchCalls) {
				harness.advance(failedWatchCalls[call-1])

				return true, nil, errSimulatedWatchFailure
			}

			watcher := watch.NewFake()

			return true, &announcingWatcher{
				FakeWatcher: watcher,
				announce:    func() { harness.watchers <- watcher },
			}, nil
		},
	)

	core, logs := observer.New(zapcore.DebugLevel)
	harness.logs = logs

	provider := kubediscovery.NewProvider(client, zap.New(core), testAPIPort,
		kubediscovery.WithErrorHook(func() { harness.reported.Add(1) }))

	base := time.Unix(0, 0)
	kubediscovery.SetClock(provider, func() time.Time {
		return base.Add(time.Duration(harness.elapsed.Load()))
	})
	kubediscovery.SetBackoff(provider, harness.backoff)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	harness.cancel = cancel

	updateCh := make(chan []string, 10)

	go func() {
		harness.errCh <- provider.Run(ctx, updateCh)
	}()

	receiveEndpoints(t, updateCh)

	return harness
}

func (h *rewatchHarness) backoff(attempt int) time.Duration {
	select {
	case h.backoffs <- attempt:
	case <-h.done:
		return 0
	}

	select {
	case <-h.resume:
	case <-h.done:
	}

	return 0
}

// advance moves the provider's clock forward. Call it only after the watch
// it should age has been delivered on watchers: delivery happens on the
// provider's first read of the stream, after it has noted the open time.
func (h *rewatchHarness) advance(d time.Duration) {
	h.elapsed.Add(int64(d))
}

func (h *rewatchHarness) nextWatcher(t *testing.T) *watch.FakeWatcher {
	t.Helper()

	select {
	case watcher := <-h.watchers:
		return watcher
	case attempt := <-h.backoffs:
		t.Fatalf("expected a new watch, got a backoff wait with attempt %d", attempt)
	case <-time.After(receiveTimeout):
		t.Fatal("timed out waiting for a watch")
	}

	return nil
}

func (h *rewatchHarness) nextBackoff(t *testing.T) int {
	t.Helper()

	select {
	case attempt := <-h.backoffs:
		h.resume <- struct{}{}

		return attempt
	case <-h.watchers:
		t.Fatal("expected a backoff wait, got an immediate re-watch")
	case <-time.After(receiveTimeout):
		t.Fatal("timed out waiting for a backoff wait")
	}

	return 0
}

func (h *rewatchHarness) stop(t *testing.T) {
	t.Helper()

	close(h.done)
	h.cancel()
	waitForRun(t, h.errCh)
}

func TestRun_StreamEndAfterHealthyLifetime_RewatchesWithoutBackoff(t *testing.T) {
	tests := []struct {
		name  string
		lived time.Duration
	}{
		{name: "exactly the healthy minimum", lived: kubediscovery.MinHealthyWatch},
		{name: "server watch timeout", lived: 45 * time.Minute},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			harness := startRewatchHarness(t)
			defer harness.stop(t)

			first := harness.nextWatcher(t)
			harness.advance(tt.lived)
			first.Stop()

			harness.nextWatcher(t)

			assert.Zero(t, harness.logs.FilterLevelExact(zapcore.WarnLevel).Len(),
				"a routine stream end must not log a warning")
			assert.Zero(t, harness.reported.Load(),
				"a routine stream end must not reach the error hook")
		})
	}
}

func TestRun_ShortLivedWatch_BacksOff(t *testing.T) {
	tests := []struct {
		name             string
		failedWatchCalls []time.Duration
		lived            time.Duration
		wantAttempts     []int
	}{
		{
			name:             "Watch call itself fails",
			failedWatchCalls: []time.Duration{0},
			wantAttempts:     []int{1},
		},
		{
			// The second failure must not reset the counter: a Watch call that
			// never returned a stream is not a healthy watch, however long it took.
			name:             "Watch call blocks for the healthy minimum, then fails",
			failedWatchCalls: []time.Duration{0, kubediscovery.MinHealthyWatch},
			wantAttempts:     []int{1, 2},
		},
		{name: "stream ends immediately", lived: 0, wantAttempts: []int{1}},
		{
			name:         "stream ends just before the healthy minimum",
			lived:        kubediscovery.MinHealthyWatch - time.Nanosecond,
			wantAttempts: []int{1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			harness := startRewatchHarness(t, tt.failedWatchCalls...)
			defer harness.stop(t)

			if len(tt.failedWatchCalls) == 0 {
				first := harness.nextWatcher(t)
				harness.advance(tt.lived)
				first.Stop()
			}

			for _, want := range tt.wantAttempts {
				assert.Equal(t, want, harness.nextBackoff(t))
			}

			assert.Equal(t, len(tt.wantAttempts),
				harness.logs.FilterMessage(logWatchErrorRestarting).Len(),
				"every backoff wait must follow a warning")
		})
	}
}

func TestRun_HealthyWatch_ResetsBackoffAttempt(t *testing.T) {
	harness := startRewatchHarness(t)
	defer harness.stop(t)

	harness.nextWatcher(t).Stop()
	require.Equal(t, 1, harness.nextBackoff(t))

	harness.nextWatcher(t).Stop()
	require.Equal(t, 2, harness.nextBackoff(t))

	healthy := harness.nextWatcher(t)
	harness.advance(kubediscovery.MinHealthyWatch)
	healthy.Error(&metav1.Status{
		Status: metav1.StatusFailure,
		Code:   500,
		Reason: metav1.StatusReasonInternalError,
	})

	assert.Equal(t, 1, harness.nextBackoff(t),
		"an error after a healthy watch must restart the backoff from the first attempt")
}

func TestRun_GoneWithSuccessfulRelist_LogsNoWarning(t *testing.T) {
	harness := startRewatchHarness(t)
	defer harness.stop(t)

	harness.nextWatcher(t).Error(&metav1.Status{
		Status: metav1.StatusFailure,
		Code:   410,
		Reason: metav1.StatusReasonExpired,
	})

	harness.nextWatcher(t)

	assert.Zero(t, harness.logs.FilterLevelExact(zapcore.WarnLevel).Len(),
		"410 Gone with a successful re-list is routine")
}

func TestRun_Watch_RequestsBookmarks(t *testing.T) {
	// Bookmarks keep the resource version fresh on a quiet cluster, so the
	// immediate re-watch after a routine stream end does not start from a
	// version the server has already forgotten.
	harness := startRewatchHarness(t)
	defer harness.stop(t)

	first := harness.nextWatcher(t)
	harness.advance(kubediscovery.MinHealthyWatch)
	first.Stop()

	harness.nextWatcher(t)

	assert.False(t, harness.noBookmarks.Load(), "every Watch call must request bookmarks")
}
