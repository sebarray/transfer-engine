package redis

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	redisclient "github.com/redis/go-redis/v9"
	"time"
	"transfer-engine/internal/business/reconciliation"
)

const leaseKey = "transfer-engine:reconciliation:lease"

type Lease struct{ client *redisclient.Client }

// New owns its client. Transport deadlines and disabled retries bound ambiguity.
func New(address string) *Lease {
	return &Lease{client: redisclient.NewClient(&redisclient.Options{Addr: address, DialTimeout: time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second, ContextTimeoutEnabled: true, MaxRetries: -1})}
}
func (l *Lease) Close() error { return l.client.Close() }
func (l *Lease) Acquire(ctx context.Context, ttl time.Duration) (string, error) {
	if ttl < time.Millisecond || ttl > time.Minute {
		return "", reconciliation.ErrInvalid
	}
	var bytes [32]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", reconciliation.ErrUnavailable
	}
	token := hex.EncodeToString(bytes[:])
	ok, err := l.client.SetNX(ctx, leaseKey, token, ttl).Result()
	if err != nil {
		return "", reconciliation.ErrUnavailable
	}
	if !ok {
		return "", reconciliation.ErrBusy
	}
	return token, nil
}
func (l *Lease) Release(ctx context.Context, token string) error {
	// An expired holder must never delete a successor's lease.
	_, err := l.client.Eval(ctx, `if redis.call('GET',KEYS[1]) == ARGV[1] then return redis.call('DEL',KEYS[1]) else return 0 end`, []string{leaseKey}, token).Result()
	if err != nil {
		return reconciliation.ErrUnavailable
	}
	return nil
}
