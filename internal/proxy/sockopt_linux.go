//go:build linux

package proxy

import (
	"net"
	"time"

	"github.com/cockroachdb/errors"
	"golang.org/x/sys/unix"
)

// setTCPUserTimeout bounds how long transmitted data may remain
// unacknowledged before the kernel kills the connection. It keeps half-open
// connections to dead upstreams from hanging forever.
func setTCPUserTimeout(conn *net.TCPConn, timeout time.Duration) error {
	if timeout <= 0 {
		return nil
	}

	raw, err := conn.SyscallConn()
	if err != nil {
		return errors.Wrap(err, "get raw connection")
	}

	var sockErr error

	err = raw.Control(func(fd uintptr) {
		sockErr = unix.SetsockoptInt(int(fd), unix.IPPROTO_TCP, unix.TCP_USER_TIMEOUT, int(timeout/time.Millisecond))
	})
	if err != nil {
		return errors.Wrap(err, "control raw connection")
	}

	return errors.Wrap(sockErr, "setsockopt TCP_USER_TIMEOUT")
}
