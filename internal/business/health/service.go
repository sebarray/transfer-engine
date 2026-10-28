// Package health checks the dependencies required to accept requests.
package health

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)

// Dependency is the readiness dependency consumed by the service.
// Ping must respect cancellation and deadlines in ctx.
type Dependency interface {
	Ping(ctx context.Context) error
}

// Readiness is the application contract exposed to readiness adapters.
type Readiness interface {
	Ready(context.Context) error
}

var _ Readiness = (*Service)(nil)

// ErrDraining reports that shutdown has stopped readiness.
var ErrDraining = errors.New("service is draining")

// Service checks readiness without coupling the business layer to a transport.
type Service struct {
	dependency Dependency
	timeout    time.Duration
	draining   atomic.Bool
}

// New constructs a service with a bounded deadline for each dependency probe.
func New(dependency Dependency, timeout time.Duration) *Service {
	return &Service{dependency: dependency, timeout: timeout}
}

// Ready succeeds when the dependency is available and shutdown has not begun.
func (s *Service) Ready(ctx context.Context) error {
	if s.draining.Load() {
		return ErrDraining
	}
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.dependency.Ping(ctx); err != nil {
		return fmt.Errorf("readiness dependency: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.draining.Load() {
		return ErrDraining
	}
	return nil
}

// Drain permanently stops readiness before the HTTP server drains requests.
func (s *Service) Drain() {
	s.draining.Store(true)
}
