// Package kubernetes provides a Kubernetes EndpointSlice-based discovery provider.
package kubernetes

import (
	"context"
	"math"
	"math/rand/v2"
	"net"
	"sort"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"

	"github.com/lexfrei/extractedprism/internal/discovery"
)

const (
	endpointSliceNamespace = "default"
	kubernetesServiceLabel = "kubernetes.io/service-name=kubernetes"
	backoffBase            = 1 * time.Second
	backoffMax             = 30 * time.Second
	backoffFactor          = 2.0
	backoffJitterFrac      = 0.25
)

// Compile-time interface check.
var _ discovery.EndpointProvider = (*Provider)(nil)

// Provider watches EndpointSlice resources for the kubernetes service.
type Provider struct {
	client       kubernetes.Interface
	logger       *zap.Logger
	apiPort      string
	knownSlices  map[string][]discoveryv1.Endpoint
	hadEndpoints bool
	onError      func()
}

// NewProvider creates a Kubernetes discovery provider.
func NewProvider(client kubernetes.Interface, logger *zap.Logger, apiPort string, opts ...Option) *Provider {
	prov := &Provider{
		client:      client,
		logger:      logger,
		apiPort:     apiPort,
		knownSlices: make(map[string][]discoveryv1.Endpoint),
		onError:     func() {},
	}

	for _, opt := range opts {
		opt(prov)
	}

	return prov
}

// Option configures a Provider.
type Option func(*Provider)

// WithErrorHook registers a callback for errors the provider recovers from
// on its own (watch restarts, failed re-lists). They never surface through
// Run's return value, so without the hook they are visible only in logs.
func WithErrorHook(hook func()) Option {
	return func(p *Provider) {
		p.onError = hook
	}
}

// ProviderName is the provider's Name and its discovery metrics label.
const ProviderName = "kubernetes"

// Name implements discovery.EndpointProvider.
func (p *Provider) Name() string {
	return ProviderName
}

// Run watches kubernetes EndpointSlices and sends updates on updateCh until ctx is done.
func (p *Provider) Run(ctx context.Context, updateCh chan<- []string) error {
	endpoints, resVer, err := p.listEndpoints(ctx)
	if err != nil {
		return errors.Wrap(err, "initial endpoint slice list")
	}

	p.hadEndpoints = len(endpoints) > 0

	select {
	case updateCh <- endpoints:
	case <-ctx.Done():
		return nil
	}

	return p.watchLoop(ctx, updateCh, resVer)
}

func (p *Provider) listEndpoints(ctx context.Context) ([]string, string, error) {
	sliceList, err := p.client.DiscoveryV1().EndpointSlices(endpointSliceNamespace).List(
		ctx, metav1.ListOptions{LabelSelector: kubernetesServiceLabel},
	)
	if err != nil {
		return nil, "", errors.Wrap(err, "list endpoint slices")
	}

	for idx := range sliceList.Items {
		slice := &sliceList.Items[idx]
		p.knownSlices[slice.Name] = copyEndpoints(slice.Endpoints)
	}

	return p.endpointsFromCache(), sliceList.ResourceVersion, nil
}

func (p *Provider) watchLoop(
	ctx context.Context,
	updateCh chan<- []string,
	resourceVersion string,
) error {
	resVer := resourceVersion
	attempt := 0

	for ctx.Err() == nil {
		watchErr := p.watchOnce(ctx, updateCh, &resVer)

		if ctx.Err() != nil {
			return nil //nolint:nilerr // context cancellation is graceful exit, watchErr is irrelevant
		}

		if watchErr == nil {
			attempt = 0

			continue
		}

		attempt++

		// Server-side stream ends and 410 Gone (etcd compaction, handled by a
		// re-list) are routine on long-lived watches; counting them would grow
		// the error metric on a healthy cluster. A failed re-list is still
		// reported below.
		if !errors.Is(watchErr, errGone) && !errors.Is(watchErr, errWatchClosed) {
			p.onError()
		}

		p.logger.Warn("watch error, restarting with backoff",
			zap.Error(watchErr), zap.Int("attempt", attempt))

		if errors.Is(watchErr, errGone) {
			if p.handleGoneRelist(ctx, updateCh, &resVer) {
				attempt = 0

				continue
			}

			if ctx.Err() != nil {
				return nil //nolint:nilerr // context cancellation is graceful exit, watchErr is irrelevant
			}
		}

		delay := BackoffDelay(attempt)
		timer := time.NewTimer(delay)

		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()

			return nil
		}
	}

	return nil
}

// handleGoneRelist performs a full re-list when Watch returns 410 Gone.
// Returns true if re-list succeeded and the caller should reset the attempt counter.
func (p *Provider) handleGoneRelist(
	ctx context.Context,
	updateCh chan<- []string,
	resVer *string,
) bool {
	p.logger.Info("resource version expired, performing full re-list")

	oldSlices := p.knownSlices
	p.knownSlices = make(map[string][]discoveryv1.Endpoint)

	endpoints, newVer, listErr := p.listEndpoints(ctx)
	if listErr != nil {
		p.knownSlices = oldSlices

		// A re-list cut short by shutdown is not a discovery failure.
		if ctx.Err() != nil {
			return false
		}

		p.onError()
		p.logger.Warn("re-list failed, retaining cached endpoints", zap.Error(listErr))

		return false
	}

	*resVer = newVer

	p.warnIfTransitionedToEmpty(endpoints)

	select {
	case updateCh <- endpoints:
		return true
	case <-ctx.Done():
		return false
	}
}

var errGone = errors.New("watch 410 Gone")

// errWatchClosed marks a watch stream that ended without an error event.
// client-go reports the server's own watch timeout and a dropped connection
// (EOF, reset, network timeout) the same way, so this covers both. Neither
// is reported: a dead upstream shows in its health_check_status, and if no
// upstream is reachable the following Watch call fails and is reported.
var errWatchClosed = errors.New("watch channel closed")

// BackoffDelay calculates exponential backoff with jitter for the given attempt number.
// Attempt must be >= 1. Jitter adds up to 25% random variation to prevent thundering herd.
func BackoffDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}

	delay := float64(backoffBase) * math.Pow(backoffFactor, float64(attempt-1))
	if delay > float64(backoffMax) {
		delay = float64(backoffMax)
	}

	jitter := delay * backoffJitterFrac * rand.Float64() //nolint:gosec // jitter does not need cryptographic randomness

	return time.Duration(delay + jitter)
}

func (p *Provider) watchOnce(
	ctx context.Context,
	updateCh chan<- []string,
	resVer *string,
) error {
	watcher, err := p.client.DiscoveryV1().EndpointSlices(endpointSliceNamespace).Watch(
		ctx, metav1.ListOptions{
			LabelSelector:   kubernetesServiceLabel,
			ResourceVersion: *resVer,
		},
	)
	if err != nil {
		return errors.Wrap(err, "watch endpoint slices")
	}

	defer watcher.Stop()

	return p.handleEvents(ctx, watcher, updateCh, resVer)
}

func (p *Provider) handleEvents(
	ctx context.Context,
	watcher watch.Interface,
	updateCh chan<- []string,
	resVer *string,
) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case event, ok := <-watcher.ResultChan():
			if !ok {
				return errWatchClosed
			}

			processErr := p.processEvent(ctx, event, updateCh, resVer)
			if processErr != nil {
				return processErr
			}
		}
	}
}

func (p *Provider) processEvent(
	ctx context.Context,
	event watch.Event,
	updateCh chan<- []string,
	resVer *string,
) error {
	switch event.Type {
	case watch.Added, watch.Modified:
		return p.handleSliceUpdate(ctx, event, updateCh, resVer)
	case watch.Deleted:
		return p.handleSliceDelete(ctx, event, updateCh, resVer)
	case watch.Error:
		if event.Object == nil {
			return errors.New("watch error event received")
		}

		status, ok := event.Object.(*metav1.Status)
		if ok && status.Code == 410 {
			return errGone
		}

		if ok {
			return errors.Errorf("watch error: status %d %s: %s",
				status.Code, status.Reason, status.Message)
		}

		return errors.New("watch error event received")
	case watch.Bookmark:
		p.updateResourceVersion(event, resVer)
	}

	return nil
}

func (p *Provider) handleSliceUpdate(
	ctx context.Context,
	event watch.Event,
	updateCh chan<- []string,
	resVer *string,
) error {
	slice, ok := event.Object.(*discoveryv1.EndpointSlice)
	if !ok {
		return errors.New("unexpected object type in watch event")
	}

	*resVer = slice.ResourceVersion
	p.knownSlices[slice.Name] = copyEndpoints(slice.Endpoints)

	extracted := p.endpointsFromCache()
	p.warnIfTransitionedToEmpty(extracted)

	select {
	case updateCh <- extracted:
	case <-ctx.Done():
		return nil
	}

	p.logger.Info("endpoints updated", zap.Strings("endpoints", extracted))

	return nil
}

func (p *Provider) handleSliceDelete(
	ctx context.Context,
	event watch.Event,
	updateCh chan<- []string,
	resVer *string,
) error {
	slice, ok := event.Object.(*discoveryv1.EndpointSlice)
	if !ok {
		return errors.New("unexpected object type in delete event")
	}

	*resVer = slice.ResourceVersion

	delete(p.knownSlices, slice.Name)

	p.logger.Warn("kubernetes endpoint slice deleted", zap.String("name", slice.Name))

	extracted := p.endpointsFromCache()
	p.warnIfTransitionedToEmpty(extracted)

	select {
	case updateCh <- extracted:
	case <-ctx.Done():
		return nil
	}

	return nil
}

func (p *Provider) updateResourceVersion(event watch.Event, resVer *string) {
	slice, ok := event.Object.(*discoveryv1.EndpointSlice)
	if !ok {
		return
	}

	*resVer = slice.ResourceVersion
}

func (p *Provider) endpointsFromCache() []string {
	seen := make(map[string]struct{})
	result := make([]string, 0)

	for _, endpoints := range p.knownSlices {
		for idx := range endpoints {
			if !isEndpointReady(&endpoints[idx]) {
				p.logger.Debug("skipping not-ready endpoint",
					zap.Strings("addresses", endpoints[idx].Addresses))

				continue
			}

			for _, addr := range endpoints[idx].Addresses {
				hostPort := net.JoinHostPort(addr, p.apiPort)
				if _, exists := seen[hostPort]; exists {
					continue
				}

				seen[hostPort] = struct{}{}

				result = append(result, hostPort)
			}
		}
	}

	sort.Strings(result)

	return result
}

// warnIfTransitionedToEmpty logs a warning when the endpoint list transitions
// from non-empty to empty (but not on repeated empty states).
func (p *Provider) warnIfTransitionedToEmpty(endpoints []string) {
	if len(endpoints) == 0 && p.hadEndpoints {
		p.logger.Warn("all endpoints removed, endpoint list is now empty")
	}

	p.hadEndpoints = len(endpoints) > 0
}

// copyEndpoints returns a deep copy of the endpoint slice to prevent
// external mutation from corrupting the internal cache.
func copyEndpoints(src []discoveryv1.Endpoint) []discoveryv1.Endpoint {
	if src == nil {
		return nil
	}

	dst := make([]discoveryv1.Endpoint, len(src))

	for i := range src {
		dst[i] = *src[i].DeepCopy()
	}

	return dst
}

// isEndpointReady returns true if the endpoint is ready to serve traffic.
// Per Kubernetes convention, nil Ready means the endpoint is ready.
func isEndpointReady(ep *discoveryv1.Endpoint) bool {
	if ep.Conditions.Ready == nil {
		return true
	}

	return *ep.Conditions.Ready
}
