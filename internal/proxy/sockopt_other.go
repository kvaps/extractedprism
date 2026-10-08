//go:build !linux

package proxy

import (
	"net"
	"time"
)

// setTCPUserTimeout is a no-op off Linux: TCP_USER_TIMEOUT does not exist
// there. The deployment target is Linux containers; other platforms are
// development-only.
func setTCPUserTimeout(_ *net.TCPConn, _ time.Duration) error {
	return nil
}
