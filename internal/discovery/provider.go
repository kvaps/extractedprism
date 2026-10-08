// Package discovery defines the endpoint discovery interface.
package discovery

import "context"

// EndpointProvider discovers and monitors backend endpoints.
type EndpointProvider interface {
	Run(ctx context.Context, updateCh chan<- []string) error
	// Name identifies the provider in logs and the discovery metrics label.
	Name() string
}
