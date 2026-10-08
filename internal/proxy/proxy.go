// Package proxy implements the TCP data path: a single-address listener
// proxying to a health-checked set of control plane endpoints, with
// connection tracking and graceful draining.
package proxy

import (
	"context"
	"io"
	"math/rand/v2"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/lexfrei/extractedprism/internal/metrics"
)

const (
	// healthFailuresToUnhealthy is the number of consecutive failed health
	// checks before a backend is excluded from picking. Two tolerates a
	// single transient failure without dropping the backend.
	healthFailuresToUnhealthy = 2

	// acceptErrorBackoff pauses the accept loop after a persistent listener
	// error to avoid a hot spin.
	acceptErrorBackoff = 100 * time.Millisecond

	// noUpstreamLabel is the upstream label value recorded when a connection
	// fails because no backend was available to dial.
	noUpstreamLabel = "none"
)

// Config holds the proxy configuration. Values are expected to be validated
// by the config package before construction.
type Config struct {
	BindAddress     string
	BindPort        int
	DialTimeout     time.Duration
	KeepAlivePeriod time.Duration
	TCPUserTimeout  time.Duration
	HealthInterval  time.Duration
	HealthTimeout   time.Duration
	// DrainTimeout bounds how long a removed backend's existing connections
	// may finish before being force-closed. Zero closes them immediately.
	DrainTimeout time.Duration
}

// trackedConn is one proxied connection: the accepted client side and the
// dialed upstream side. Closing either side ends both copy directions.
type trackedConn struct {
	client   net.Conn
	upstream net.Conn
	once     sync.Once
}

func (tc *trackedConn) close() {
	tc.once.Do(func() {
		tc.client.Close()
		tc.upstream.Close()
	})
}

// backend is one upstream endpoint with its health state and the set of
// connections currently proxied to it.
type backend struct {
	addr       string
	healthy    atomic.Bool
	draining   atomic.Bool
	failures   atomic.Int32
	stopHealth chan struct{}

	mu sync.Mutex
	// conns holds the connections currently proxied to this backend.
	conns map[*trackedConn]struct{}
	// emptyCh is non-nil only while draining with open connections and is
	// closed once conns becomes empty. A new channel is created per epoch.
	emptyCh chan struct{}
	// abortCh is closed when a drain is aborted by re-adding the backend.
	abortCh chan struct{}
	// drainGen identifies the current drain epoch; a stale drainer from an
	// older epoch must not act on the backend.
	drainGen atomic.Int64
}

// Proxy accepts TCP connections on one address and forwards them to healthy
// backends. The backend set is reconciled from endpoint updates.
type Proxy struct {
	cfg     Config
	logger  *zap.Logger
	metrics *metrics.Metrics

	ctx       context.Context //nolint:containedctx // lifecycle owned by Start/Shutdown, not a call chain
	cancel    context.CancelFunc
	closeOnce sync.Once
	wg        sync.WaitGroup

	mu       sync.RWMutex
	backends map[string]*backend
	listener net.Listener
}

// New creates a Proxy. Panics if logger or metrics is nil.
func New(cfg Config, logger *zap.Logger, m *metrics.Metrics) *Proxy {
	if logger == nil {
		panic("proxy.New: logger must not be nil")
	}

	if m == nil {
		panic("proxy.New: metrics must not be nil")
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Proxy{
		cfg:      cfg,
		logger:   logger,
		metrics:  m,
		ctx:      ctx,
		cancel:   cancel,
		backends: make(map[string]*backend),
	}
}

// Start binds the listener and launches the accept and reconcile loops.
// BindPort 0 selects an ephemeral port; use Addr to discover it.
func (prx *Proxy) Start(ctx context.Context, updates <-chan []string) error {
	listenAddr := net.JoinHostPort(prx.cfg.BindAddress, strconv.Itoa(prx.cfg.BindPort))

	lc := net.ListenConfig{}

	listener, err := lc.Listen(ctx, "tcp", listenAddr)
	if err != nil {
		return errors.Wrap(err, "proxy listen")
	}

	prx.mu.Lock()
	prx.listener = listener
	prx.cancel() // release the placeholder context created by New
	prx.ctx, prx.cancel = context.WithCancel(ctx)
	prx.mu.Unlock()

	prx.logger.Info("proxy listening", zap.String("addr", listener.Addr().String()))

	prx.wg.Add(2)

	go prx.acceptLoop()
	go prx.reconcileLoop(updates)

	return nil
}

// Addr returns the bound listener address, or "" before Start.
func (prx *Proxy) Addr() string {
	prx.mu.RLock()
	defer prx.mu.RUnlock()

	if prx.listener == nil {
		return ""
	}

	return prx.listener.Addr().String()
}

// Healthy reports whether at least one backend is pickable (healthy and not
// draining).
func (prx *Proxy) Healthy() bool {
	prx.mu.RLock()
	defer prx.mu.RUnlock()

	for _, bck := range prx.backends {
		if bck.healthy.Load() && !bck.draining.Load() {
			return true
		}
	}

	return false
}

// Shutdown stops accepting, aborts health checks and drains, closes all
// in-flight connections, and waits for the goroutines to exit. The wait is
// bounded by ctx.
func (prx *Proxy) Shutdown(ctx context.Context) error {
	prx.closeOnce.Do(func() {
		prx.mu.Lock()

		if prx.cancel != nil {
			prx.cancel()
		}

		if prx.listener != nil {
			prx.listener.Close()
		}

		for _, bck := range prx.backends {
			bck.closeAllConns()
		}
		prx.mu.Unlock()
	})

	waitCh := make(chan struct{})

	go func() {
		prx.wg.Wait()
		close(waitCh)
	}()

	select {
	case <-waitCh:
		return nil
	case <-ctx.Done():
		return errors.Wrap(ctx.Err(), "proxy shutdown wait")
	}
}

func (prx *Proxy) acceptLoop() {
	defer prx.wg.Done()

	for {
		conn, err := prx.listener.Accept()
		if err != nil {
			if prx.ctx.Err() != nil {
				return
			}

			prx.logger.Warn("accept error", zap.Error(err))

			select {
			case <-prx.ctx.Done():
				return
			case <-time.After(acceptErrorBackoff):
			}

			continue
		}

		prx.wg.Add(1)

		go prx.handleConn(conn)
	}
}

func (prx *Proxy) reconcileLoop(updates <-chan []string) {
	defer prx.wg.Done()

	for {
		select {
		case <-prx.ctx.Done():
			return
		case addrs, ok := <-updates:
			// A closed channel means no more discovery input, not "no
			// endpoints": keep serving the current backends.
			if !ok {
				return
			}

			prx.reconcile(addrs)
		}
	}
}

// reconcile applies a new endpoint list: unknown addresses are added, missing
// addresses start draining, re-appearing addresses abort their drain.
func (prx *Proxy) reconcile(addrs []string) {
	prx.mu.Lock()
	defer prx.mu.Unlock()

	wanted := make(map[string]struct{}, len(addrs))
	for _, addr := range addrs {
		wanted[addr] = struct{}{}
	}

	for addr := range wanted {
		bck, ok := prx.backends[addr]
		if ok {
			if bck.draining.Load() {
				bck.cancelDrain()
				stopHealth := bck.restartHealthLoop()

				prx.wg.Add(1)

				go prx.healthLoop(bck, stopHealth)
			}

			continue
		}

		stopHealth := make(chan struct{})
		bck = &backend{
			addr:       addr,
			stopHealth: stopHealth,
			conns:      make(map[*trackedConn]struct{}),
		}
		bck.healthy.Store(true) // optimistic: pickable until proven dead
		prx.backends[addr] = bck
		prx.metrics.SetBackendHealth(addr, true)

		prx.wg.Add(1)

		go prx.healthLoop(bck, stopHealth)
	}

	for addr, bck := range prx.backends {
		if _, ok := wanted[addr]; ok || bck.draining.Load() {
			continue
		}

		emptyCh, abortCh, gen := bck.startDrain()

		prx.logger.Info("draining upstream",
			zap.String("upstream", addr),
			zap.Int("connections", bck.connCount()),
			zap.Duration("timeout", prx.cfg.DrainTimeout))

		prx.wg.Add(1)

		go prx.drainBackend(bck, emptyCh, abortCh, gen)
	}

	prx.updateUpstreamMetricsLocked()
}

// drainBackend waits for a removed backend's connections to finish, up to
// DrainTimeout, then removes the backend. The channels and generation belong
// to one drain epoch: a re-added backend aborts via abortCh, and a stale
// drainer from an older epoch is disarmed by the generation check.
func (prx *Proxy) drainBackend(bck *backend, emptyCh, abortCh <-chan struct{}, gen int64) {
	defer prx.wg.Done()

	timer := time.NewTimer(prx.cfg.DrainTimeout)
	defer timer.Stop()

	force := false

	select {
	case <-emptyCh:
	case <-timer.C:
		force = true
	case <-abortCh:
		return
	case <-prx.ctx.Done():
		return
	}

	if force {
		remaining := bck.forceCloseIfDraining(gen)
		if remaining < 0 {
			return // drain was aborted or superseded concurrently
		}

		if remaining > 0 {
			prx.logger.Warn("drain timeout exceeded, force closed connections",
				zap.String("upstream", bck.addr),
				zap.Int("remaining", remaining))
		}
	}

	prx.removeBackend(bck, gen)
}

// removeBackend deletes a drained backend from the set. It is a no-op when
// the drain was aborted (re-added), superseded by a newer drain epoch, or the
// map already holds a different backend for the address.
func (prx *Proxy) removeBackend(bck *backend, gen int64) {
	prx.mu.Lock()
	defer prx.mu.Unlock()

	if !bck.draining.Load() || bck.drainGen.Load() != gen {
		return
	}

	current, ok := prx.backends[bck.addr]
	if !ok || current != bck {
		return
	}

	delete(prx.backends, bck.addr)
	prx.metrics.RemoveBackend(bck.addr)
	prx.updateUpstreamMetricsLocked()

	prx.logger.Info("upstream drained and removed", zap.String("upstream", bck.addr))
}

// healthLoop periodically TCP-dials the backend. Two consecutive failures
// mark it unhealthy; one success marks it healthy again.
//
// stopHealth is the channel of the epoch the loop was spawned for. It is
// passed in rather than read from the backend: a remove and re-add can
// replace the field before this goroutine is scheduled, and reading it then
// would bind two loops to the same backend.
func (prx *Proxy) healthLoop(bck *backend, stopHealth <-chan struct{}) {
	defer prx.wg.Done()

	// First check runs at once: with the two-failure threshold it moves
	// exclusion one full interval earlier than a ticker-only loop.
	prx.checkOnce(prx.ctx, bck)

	ticker := time.NewTicker(prx.cfg.HealthInterval)
	defer ticker.Stop()

	for {
		select {
		case <-stopHealth:
			return
		case <-prx.ctx.Done():
			return
		case <-ticker.C:
			prx.checkOnce(prx.ctx, bck)
		}
	}
}

func (prx *Proxy) checkOnce(ctx context.Context, bck *backend) {
	dialer := net.Dialer{Timeout: prx.cfg.HealthTimeout}

	conn, err := dialer.DialContext(ctx, "tcp", bck.addr)
	if err != nil {
		prx.recordFailure(bck, err)

		return
	}

	conn.Close()

	prx.recordSuccess(bck)
}

// recordFailure feeds one failed contact with the backend (health check or
// proxied dial) into the shared exclusion accounting: client-facing dial
// errors must downgrade a backend just like health check failures do, and
// successful contacts of either kind reset the streak via recordSuccess.
//
// Contacts observed after shutdown began are ignored, here and in
// recordSuccess: they say nothing about the upstream and would log false
// transitions.
func (prx *Proxy) recordFailure(bck *backend, err error) {
	if prx.ctx.Err() != nil {
		return
	}

	if bck.failures.Add(1) < healthFailuresToUnhealthy || !bck.healthy.Swap(false) {
		return
	}

	prx.logger.Warn("upstream became unhealthy",
		zap.String("upstream", bck.addr), zap.Error(err))
	prx.onHealthChange(bck)
}

func (prx *Proxy) recordSuccess(bck *backend) {
	if prx.ctx.Err() != nil {
		return
	}

	bck.failures.Store(0)

	if bck.healthy.Swap(true) {
		return
	}

	prx.logger.Info("upstream became healthy", zap.String("upstream", bck.addr))
	prx.onHealthChange(bck)
}

// onHealthChange publishes the backend's current health state. It reads the
// state instead of taking it as an argument: two racing transitions can reach
// this point out of order, and a passed-in value could be stale.
func (prx *Proxy) onHealthChange(bck *backend) {
	prx.mu.Lock()
	defer prx.mu.Unlock()

	// A health check in flight while its backend was removed must not
	// resurrect the deleted metric series or skew the upstream counts.
	if prx.backends[bck.addr] != bck {
		return
	}

	prx.metrics.SetBackendHealth(bck.addr, bck.healthy.Load())
	prx.updateUpstreamMetricsLocked()
}

// updateUpstreamMetricsLocked refreshes the upstream count gauges.
// Callers must hold prx.mu.
func (prx *Proxy) updateUpstreamMetricsLocked() {
	active := 0

	for _, bck := range prx.backends {
		if bck.healthy.Load() && !bck.draining.Load() {
			active++
		}
	}

	prx.metrics.SetUpstreams(active, len(prx.backends))
}

// pickBackend returns a random pickable backend, or nil when none is
// available. Draining backends are never picked: they serve existing
// connections only.
func (prx *Proxy) pickBackend() *backend {
	prx.mu.RLock()
	defer prx.mu.RUnlock()

	var pickable []*backend

	for _, bck := range prx.backends {
		if bck.healthy.Load() && !bck.draining.Load() {
			pickable = append(pickable, bck)
		}
	}

	if len(pickable) == 0 {
		return nil
	}

	//nolint:gosec // load balancing pick is not security-sensitive
	return pickable[rand.IntN(len(pickable))]
}

func (prx *Proxy) handleConn(client net.Conn) {
	defer prx.wg.Done()

	prx.tuneClientConn(client)

	bck := prx.pickOrClose(client)
	if bck == nil {
		return
	}

	prx.dialAndServe(client, bck, true)
}

// pickOrClose returns a pickable backend, or closes the client and returns
// nil when there is none.
func (prx *Proxy) pickOrClose(client net.Conn) *backend {
	bck := prx.pickBackend()
	if bck == nil {
		prx.metrics.ConnError(noUpstreamLabel)
		prx.logger.Warn("no pickable upstream, closing connection",
			zap.String("client", client.RemoteAddr().String()))
		client.Close()
	}

	return bck
}

// mayRetry allows one more pick when the backend starts draining mid-dial.
func (prx *Proxy) dialAndServe(client net.Conn, bck *backend, mayRetry bool) {
	upstream, err := prx.dialUpstream(bck.addr)
	if err != nil {
		client.Close()

		// A dial failing during shutdown says nothing about the upstream.
		if prx.ctx.Err() != nil {
			return
		}

		prx.recordFailure(bck, err)
		prx.connError(bck)
		prx.logger.Warn("upstream dial failed",
			zap.String("upstream", bck.addr), zap.Error(err))

		return
	}

	prx.recordSuccess(bck)

	prx.serveConn(bck, &trackedConn{client: client, upstream: upstream}, mayRetry)
}

// serveConn registers a dialed connection with its backend and relays it
// until both directions end.
func (prx *Proxy) serveConn(bck *backend, tconn *trackedConn, mayRetry bool) {
	// A connection dialed while its backend was being removed must not enter
	// service: the drain bookkeeping may already be complete, and the
	// connection would escape both force-close and shutdown accounting.
	if !prx.tryRegister(bck, tconn) {
		// Outside shutdown the refusal means the backend started draining
		// mid-dial: the client is still good for another backend.
		if prx.ctx.Err() == nil && mayRetry {
			prx.retry(bck, tconn)

			return
		}

		tconn.close()

		if prx.ctx.Err() == nil {
			prx.connError(bck)
		}

		return
	}

	prx.metrics.ConnOpened()

	prx.relay(tconn)

	prx.metrics.ConnClosed()
	bck.unregister(tconn)
}

// retry drops the unregistered upstream side of a refused connection and
// serves the client from one more pick, without further retries.
func (prx *Proxy) retry(refused *backend, tconn *trackedConn) {
	tconn.upstream.Close()

	prx.logger.Info("upstream started draining during dial, retrying",
		zap.String("upstream", refused.addr))

	bck := prx.pickOrClose(tconn.client)
	if bck == nil {
		return
	}

	prx.dialAndServe(tconn.client, bck, false)
}

// connError records a failed connection while its address is in the set. The
// series is per address, so a re-added address counts again. The read lock
// spans the increment: removeBackend cannot delete the series in between, and
// a late error cannot recreate it.
func (prx *Proxy) connError(bck *backend) {
	prx.mu.RLock()
	defer prx.mu.RUnlock()

	_, ok := prx.backends[bck.addr]
	if !ok {
		return
	}

	prx.metrics.ConnError(bck.addr)
}

// relay copies both directions until both have ended, then closes the
// connection. A clean EOF in one direction only half-closes the peer's write
// side: a client that sends a request and calls CloseWrite must still get the
// response. A copy error tears down both sides at once.
func (prx *Proxy) relay(tconn *trackedConn) {
	var copyWg sync.WaitGroup

	copyWg.Go(func() { pipe(tconn, tconn.upstream, tconn.client) })
	copyWg.Go(func() { pipe(tconn, tconn.client, tconn.upstream) })

	copyWg.Wait()
	tconn.close()
}

func pipe(tconn *trackedConn, dst, src net.Conn) {
	_, err := io.Copy(dst, src)
	if err != nil {
		tconn.close()

		return
	}

	halfCloser, ok := dst.(interface{ CloseWrite() error })
	if !ok {
		tconn.close()

		return
	}

	err = halfCloser.CloseWrite()
	if err != nil {
		tconn.close()
	}
}

// tuneClientConn gives the accepted side the same dead-peer bounds as the
// upstream side: without them a vanished client pins its upstream
// connection for the kernel's full retransmission timeout.
func (prx *Proxy) tuneClientConn(client net.Conn) {
	tcpConn, ok := client.(*net.TCPConn)
	if !ok {
		return
	}

	// Idle only, like the dialer's KeepAlive on the upstream side: probe
	// interval and count stay at Go's defaults on both sides.
	err := tcpConn.SetKeepAliveConfig(net.KeepAliveConfig{Enable: true, Idle: prx.cfg.KeepAlivePeriod})
	if err != nil {
		prx.logger.Warn("failed to set client keepalive", zap.Error(err))
	}

	err = setTCPUserTimeout(tcpConn, prx.cfg.TCPUserTimeout)
	if err != nil {
		prx.logger.Warn("failed to set client TCP_USER_TIMEOUT", zap.Error(err))
	}
}

func (prx *Proxy) dialUpstream(addr string) (net.Conn, error) {
	dialer := net.Dialer{
		Timeout:   prx.cfg.DialTimeout,
		KeepAlive: prx.cfg.KeepAlivePeriod,
	}

	conn, err := dialer.DialContext(prx.ctx, "tcp", addr)
	if err != nil {
		return nil, errors.Wrap(err, "dial upstream")
	}

	// Dial with the "tcp" network always returns *net.TCPConn; the guard only
	// keeps the type assertion safe against future refactors.
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		return conn, nil
	}

	err = setTCPUserTimeout(tcpConn, prx.cfg.TCPUserTimeout)
	if err != nil {
		prx.logger.Warn("failed to set TCP_USER_TIMEOUT",
			zap.String("upstream", addr), zap.Error(err))
	}

	return conn, nil
}

// startDrain marks the backend draining and returns the channels and
// generation of the new drain epoch; the caller hands them to drainBackend.
// With no open connections the returned empty channel is already closed, so
// the drain completes immediately instead of waiting out the timeout.
// Callers reconcile under prx.mu and guard with the draining flag.
func (bck *backend) startDrain() (<-chan struct{}, <-chan struct{}, int64) {
	bck.mu.Lock()
	defer bck.mu.Unlock()

	bck.draining.Store(true)
	bck.drainGen.Add(1)

	empty := make(chan struct{})
	abort := make(chan struct{})
	bck.abortCh = abort

	if len(bck.conns) == 0 {
		close(empty)
	} else {
		bck.emptyCh = empty
	}

	close(bck.stopHealth)

	return empty, abort, bck.drainGen.Load()
}

// cancelDrain aborts an in-flight drain when the backend re-appears in the
// endpoint list. Safe to call on non-draining backends.
func (bck *backend) cancelDrain() {
	bck.mu.Lock()
	defer bck.mu.Unlock()

	if !bck.draining.Load() {
		return
	}

	bck.draining.Store(false)

	if bck.abortCh != nil {
		close(bck.abortCh)
		bck.abortCh = nil
	}

	bck.emptyCh = nil
}

// restartHealthLoop installs a fresh stop channel after an aborted drain
// (the drain start closed the previous one) and returns it for the caller
// to hand to the new healthLoop goroutine.
func (bck *backend) restartHealthLoop() <-chan struct{} {
	bck.mu.Lock()
	defer bck.mu.Unlock()

	bck.stopHealth = make(chan struct{})

	return bck.stopHealth
}

// forceCloseIfDraining closes all tracked connections when the backend is
// still draining in the given epoch. Returns the number of closed
// connections, or -1 when the drain was already aborted or superseded.
func (bck *backend) forceCloseIfDraining(gen int64) int {
	bck.mu.Lock()
	defer bck.mu.Unlock()

	if !bck.draining.Load() || bck.drainGen.Load() != gen {
		return -1
	}

	remaining := len(bck.conns)

	for tconn := range bck.conns {
		tconn.close()
	}

	return remaining
}

func (bck *backend) closeAllConns() {
	bck.mu.Lock()
	defer bck.mu.Unlock()

	for tconn := range bck.conns {
		tconn.close()
	}
}

// tryRegister starts tracking a proxied connection. It fails when the proxy
// is shutting down or the backend is draining: such a connection would escape
// both drain force-close and shutdown accounting. The context check runs
// under bck.mu, which closeAllConns also takes, and cancel always precedes
// closeAllConns in Shutdown, so no connection slips between the two.
func (prx *Proxy) tryRegister(bck *backend, tconn *trackedConn) bool {
	bck.mu.Lock()
	defer bck.mu.Unlock()

	if prx.ctx.Err() != nil {
		return false
	}

	if bck.draining.Load() {
		return false
	}

	bck.conns[tconn] = struct{}{}

	return true
}

// unregister stops tracking a connection and closes the drain epoch's empty
// channel when the last connection is gone.
func (bck *backend) unregister(tconn *trackedConn) {
	bck.mu.Lock()
	defer bck.mu.Unlock()

	delete(bck.conns, tconn)

	if bck.emptyCh != nil && len(bck.conns) == 0 {
		close(bck.emptyCh)
		bck.emptyCh = nil
	}
}

func (bck *backend) connCount() int {
	bck.mu.Lock()
	defer bck.mu.Unlock()

	return len(bck.conns)
}
