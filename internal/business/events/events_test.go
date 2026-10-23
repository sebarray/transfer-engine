package events

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func fixture() Packet {
	e := Envelope{ID: "00000000-0000-0000-0000-000000000001", Type: "transfer.completed", Version: 1, AggregateID: "00000000-0000-0000-0000-000000000002", OccurredAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), Payload: Payload{TransferID: "00000000-0000-0000-0000-000000000002", Currency: "ARS", Amount: 1}}
	body, _ := json.Marshal(e)
	return Packet{ID: e.ID, Body: body}
}

type effect struct {
	err   error
	calls int
}

func (e *effect) Apply(context.Context, Envelope) error { e.calls++; return e.err }

type sender struct {
	err    error
	route  string
	packet Packet
	calls  int
}

func (s *sender) Publish(_ context.Context, route string, p Packet) error {
	s.calls++
	s.route = route
	s.packet = p
	return s.err
}

type delivery struct {
	acks int
	err  error
}

func (d *delivery) Ack() error { d.acks++; return d.err }
func TestAcknowledgementAndRouting(t *testing.T) {
	for _, tc := range []struct {
		name               string
		attempt            int
		bad                bool
		effectErr, sendErr error
		route              string
		acks               int
	}{
		{"committed effect", 0, false, nil, nil, "", 1},
		{"poison", 0, true, nil, nil, DeadQueue, 1},
		{"permanent effect", 0, false, ErrPermanent, nil, DeadQueue, 1},
		{"retry one", 0, false, ErrTransient, nil, RetryQueue(1), 1},
		{"retry two", 1, false, ErrTransient, nil, RetryQueue(2), 1},
		{"retry three", 2, false, ErrTransient, nil, RetryQueue(3), 1},
		{"exhausted", 3, false, ErrTransient, nil, DeadQueue, 1},
		{"negative attempt", -1, false, nil, nil, DeadQueue, 1},
		{"uncertain retry handoff", 0, false, ErrTransient, ErrHandoff, RetryQueue(1), 0},
		{"uncertain DLQ handoff", 3, false, ErrTransient, ErrHandoff, DeadQueue, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := fixture()
			p.Attempt = tc.attempt
			if tc.bad {
				p.Body = []byte(`{"event_version":2}`)
			}
			e, s, d := &effect{err: tc.effectErr}, &sender{err: tc.sendErr}, &delivery{}
			policy := DefaultPolicy()
			policy.Jitter = func(d time.Duration) time.Duration { return d }
			err := (Consumer{Effects: e, Sender: s, Policy: policy}).Handle(context.Background(), p, d)
			if d.acks != tc.acks || s.route != tc.route {
				t.Fatalf("acks=%d route=%s err=%v", d.acks, s.route, err)
			}
			if tc.sendErr != nil && !errors.Is(err, ErrHandoff) {
				t.Fatal(err)
			}
			if tc.bad && e.calls != 0 {
				t.Fatal("poison event reached effect")
			}
			if tc.route != "" && s.packet.ID != p.ID {
				t.Fatal("handoff changed message ID")
			}
			if tc.route == RetryQueue(1) && s.packet.Delay != 5*time.Second {
				t.Fatal("wrong retry delay")
			}
		})
	}
}
func TestValidation(t *testing.T) {
	base, _ := Decode(fixture().Body)
	for _, change := range []func(*Envelope){func(e *Envelope) { e.Version = 2 }, func(e *Envelope) { e.ID = "invalid" }, func(e *Envelope) { e.Payload.Amount = 0 }, func(e *Envelope) { e.Payload.Currency = "ZZZ" }, func(e *Envelope) { e.Payload.TransferID = e.ID }, func(e *Envelope) { e.OccurredAt = time.Time{} }} {
		e := base
		change(&e)
		body, _ := json.Marshal(e)
		if _, err := Decode(body); !errors.Is(err, ErrPermanent) {
			t.Fatal("invalid event accepted")
		}
	}
	if _, err := Decode(append(fixture().Body, []byte(` {}`)...)); err == nil {
		t.Fatal("multiple JSON values accepted")
	}
	if _, err := Decode(make([]byte, MaxBody+1)); err == nil {
		t.Fatal("oversized event accepted")
	}
}
func TestJitterBounds(t *testing.T) {
	for i := 0; i < 100; i++ {
		d := Jitter(time.Second)
		if d < 800*time.Millisecond || d > 1200*time.Millisecond {
			t.Fatal(d)
		}
	}
}

type audit struct {
	began, confirmed int
	err              error
}

func (a *audit) BeginReplay(context.Context, string, string, string) (string, error) {
	a.began++
	return "audit", a.err
}
func (a *audit) ConfirmReplay(context.Context, string) error { a.confirmed++; return a.err }
func TestReplaySafety(t *testing.T) {
	for _, dry := range []bool{true, false} {
		s, d, a := &sender{}, &delivery{}, &audit{}
		p := fixture()
		p.Attempt = 3
		if err := Replay(context.Background(), p, d, s, a, ReplayRequest{Operator: "operator", Reason: "dependency restored", DryRun: dry}); err != nil {
			t.Fatal(err)
		}
		if dry {
			if s.calls != 0 || d.acks != 0 || a.began != 0 {
				t.Fatal("dry run mutated state")
			}
		} else if d.acks != 1 || a.confirmed != 1 || s.packet.Attempt != 0 || s.packet.Replays != 1 || s.packet.ID != p.ID {
			t.Fatal("unsafe replay")
		}
	}
	s, d, a := &sender{err: ErrHandoff}, &delivery{}, &audit{}
	if err := Replay(context.Background(), fixture(), d, s, a, ReplayRequest{Operator: "operator", Reason: "retry"}); !errors.Is(err, ErrHandoff) || d.acks != 0 || a.confirmed != 0 {
		t.Fatal("unconfirmed replay acknowledged")
	}
}
