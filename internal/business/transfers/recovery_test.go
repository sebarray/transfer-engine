package transfers

import (
	"context"
	"errors"
	"testing"
	"time"
)

type uncertainStore struct {
	fakeStore
	recovered             *Result
	resolveErr            error
	resolutions, attempts int
	claim                 Claim
}

func (s *uncertainStore) WithinTransaction(_ context.Context, fn func(Tx) error) error {
	s.attempts++
	return ErrCommitUnknown
}
func (s *uncertainStore) Resolve(ctx context.Context, c Claim) (*Result, error) {
	s.resolutions++
	s.claim = c
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if _, ok := ctx.Deadline(); !ok {
		return nil, errors.New("unbounded resolution")
	}
	return s.recovered, s.resolveErr
}
func TestAmbiguousCommitResolutionNeverReexecutes(t *testing.T) {
	for _, tc := range []struct {
		name          string
		result        *Result
		failure, want error
	}{
		{"committed", &Result{ID: "durable", Status: "completed"}, nil, nil},
		{"absent remains unknown", nil, nil, ErrCommitUnknown},
		{"unavailable remains unknown", nil, ErrUnavailable, ErrCommitUnknown},
		{"mismatch", nil, ErrMismatch, ErrMismatch},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &uncertainStore{recovered: tc.result, resolveErr: tc.failure}
			service, _ := New(store, "test", false)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			result, err := service.Transfer(ctx, Command{ActorID: "alice", SourceID: "aaaaaaaa-0000-0000-0000-000000000001", DestinationID: "bbbbbbbb-0000-0000-0000-000000000002", Currency: "ARS", Amount: 1, Key: "key"})
			if !errors.Is(err, tc.want) {
				t.Fatalf("got %v want %v", err, tc.want)
			}
			if store.attempts != 1 || store.resolutions != 1 {
				t.Fatal("unexpected retry or missing resolution")
			}
			if tc.result != nil && (!result.Replayed || result.ID != "durable") {
				t.Fatal(result)
			}
		})
	}
}
func TestCanonicalFingerprint(t *testing.T) {
	store := &uncertainStore{}
	service, _ := New(store, "test", false)
	c := Command{ActorID: "alice", SourceID: "AAAAAAAA-0000-0000-0000-000000000001", DestinationID: "BBBBBBBB-0000-0000-0000-000000000002", Currency: "ARS", Amount: 1, Key: "key"}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, _ = service.Transfer(ctx, c)
	first := store.claim.Hash
	c.SourceID = "aaaaaaaa-0000-0000-0000-000000000001"
	c.DestinationID = "bbbbbbbb-0000-0000-0000-000000000002"
	c.Key = "other-key"
	_, _ = service.Transfer(ctx, c)
	if first != store.claim.Hash {
		t.Fatal("equivalent UUIDs or key changed payload fingerprint")
	}
	c.Amount = 2
	_, _ = service.Transfer(ctx, c)
	if first == store.claim.Hash {
		t.Fatal("amount did not change fingerprint")
	}
}
