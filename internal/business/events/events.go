// Package events defines the auxiliary transfer notification contract.
package events

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math/rand/v2"
	"strings"
	"time"
)

const (
	MainQueue = "transfer.completed.v1"
	DeadQueue = "transfer.completed.v1.dlq"
	MaxBody   = 64 << 10
)

var (
	ErrPermanent = errors.New("invalid or unsupported event")
	ErrTransient = errors.New("event dependency unavailable")
	ErrHandoff   = errors.New("message handoff not confirmed")
)

type Payload struct {
	TransferID string `json:"transfer_id"`
	Currency   string `json:"currency"`
	Amount     int64  `json:"amount_minor"`
}
type Envelope struct {
	ID          string    `json:"event_id"`
	Type        string    `json:"event_type"`
	Version     int       `json:"event_version"`
	AggregateID string    `json:"aggregate_id"`
	OccurredAt  time.Time `json:"occurred_at"`
	Traceparent string    `json:"traceparent,omitempty"`
	Payload     Payload   `json:"payload"`
}

func uuid(value string) bool {
	if len(value) != 36 || value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' || value != strings.ToLower(value) {
		return false
	}
	data, err := hex.DecodeString(strings.ReplaceAll(value, "-", ""))
	return err == nil && len(data) == 16
}
func Decode(body []byte) (Envelope, error) {
	var e Envelope
	if len(body) == 0 || len(body) > MaxBody {
		return e, ErrPermanent
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&e); err != nil {
		return e, ErrPermanent
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		return e, ErrPermanent
	}
	if !uuid(e.ID) || !uuid(e.AggregateID) || e.Type != "transfer.completed" || e.Version != 1 || e.OccurredAt.IsZero() || e.Payload.TransferID != e.AggregateID || e.Payload.Amount <= 0 {
		return e, ErrPermanent
	}
	if e.Payload.Currency != "ARS" && e.Payload.Currency != "USD" && e.Payload.Currency != "EUR" {
		return e, ErrPermanent
	}
	if len(e.Traceparent) > 256 {
		return e, ErrPermanent
	}
	return e, nil
}

type Packet struct {
	Body             []byte
	ID               string
	Attempt, Replays int
	Reason           string
	Delay            time.Duration
}
type Sender interface {
	Publish(context.Context, string, Packet) error
}
type Effects interface {
	Apply(context.Context, Envelope) error
}
type Outbox interface {
	DispatchOne(context.Context, func(context.Context, Envelope) error) (bool, error)
}
type Delivery interface{ Ack() error }
type RetryPolicy struct {
	Delays [3]time.Duration
	Jitter func(time.Duration) time.Duration
}

func DefaultPolicy() RetryPolicy {
	return RetryPolicy{Delays: [3]time.Duration{5 * time.Second, 30 * time.Second, 2 * time.Minute}, Jitter: Jitter}
}

// Jitter is bounded to [80%,120%]; TTL queues may delay a message behind an older one.
func Jitter(delay time.Duration) time.Duration {
	if delay <= 0 {
		return time.Millisecond
	}
	return delay*8/10 + time.Duration(rand.Int64N(int64(delay*4/10)+1))
}
func RetryQueue(attempt int) string { return MainQueue + ".retry." + string(rune('0'+attempt)) }

type Consumer struct {
	Effects Effects
	Sender  Sender
	Policy  RetryPolicy
}

// Handle ACKs only a committed effect or a confirmed durable handoff. On error
// the transport must close/requeue and back off, never spin on immediate NACKs.
func (c Consumer) Handle(ctx context.Context, p Packet, d Delivery) error {
	envelope, err := Decode(p.Body)
	if p.Attempt < 0 || p.Attempt > 3 || p.Replays < 0 || p.Replays > 5 {
		err = ErrPermanent
	}
	if err == nil {
		err = c.Effects.Apply(ctx, envelope)
	}
	if err == nil {
		return d.Ack()
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	route := DeadQueue
	p.Delay = 0
	switch {
	case errors.Is(err, ErrPermanent):
		p.Reason = "invalid_event"
	case p.Attempt >= 3:
		p.Reason = "retry_exhausted"
	default:
		p.Attempt++
		p.Reason = "transient_dependency"
		route = RetryQueue(p.Attempt)
		delay := c.Policy.Delays[p.Attempt-1]
		if c.Policy.Jitter != nil {
			delay = c.Policy.Jitter(delay)
		}
		p.Delay = delay
	}
	if p.Attempt < 0 || p.Attempt > 3 {
		p.Attempt = 0
	}
	if p.Replays < 0 || p.Replays > 5 {
		p.Replays = 0
	}
	if err = c.Sender.Publish(ctx, route, p); err != nil {
		return ErrHandoff
	}
	return d.Ack()
}
func PublishNext(ctx context.Context, store Outbox, sender Sender) (bool, error) {
	return store.DispatchOne(ctx, func(ctx context.Context, e Envelope) error {
		body, err := json.Marshal(e)
		if err != nil {
			return ErrPermanent
		}
		// Send even unsupported versions to the consumer, which durably quarantines
		// them in DLQ instead of leaving one poison row at the head of the outbox.
		return sender.Publish(ctx, MainQueue, Packet{ID: e.ID, Body: body})
	})
}

type ReplayAudit interface {
	BeginReplay(context.Context, string, string, string) (string, error)
	ConfirmReplay(context.Context, string) error
}
type ReplayRequest struct {
	Operator, Reason string
	DryRun           bool
}

func Replay(ctx context.Context, p Packet, d Delivery, s Sender, audit ReplayAudit, request ReplayRequest) error {
	if strings.TrimSpace(request.Operator) == "" || len(request.Operator) > 200 || strings.TrimSpace(request.Reason) == "" || len(request.Reason) > 500 {
		return ErrPermanent
	}
	e, err := Decode(p.Body)
	if err != nil {
		return err
	}
	if p.Replays < 0 || p.Replays >= 5 {
		return ErrPermanent
	}
	if request.DryRun {
		return nil
	}
	id, err := audit.BeginReplay(ctx, e.ID, request.Operator, request.Reason)
	if err != nil {
		return err
	}
	p.ID = e.ID
	p.Attempt = 0
	p.Replays++
	p.Reason = "operator_replay"
	p.Delay = 0
	if err = s.Publish(ctx, MainQueue, p); err != nil {
		return ErrHandoff
	}
	if err = audit.ConfirmReplay(ctx, id); err != nil {
		return err
	}
	return d.Ack()
}
