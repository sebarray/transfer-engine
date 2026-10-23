package reconciliation

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakeLease struct {
	failure  error
	releases int
}

func (f *fakeLease) Acquire(context.Context, time.Duration) (string, error) {
	return "token", f.failure
}
func (f *fakeLease) Release(context.Context, string) error { f.releases++; return nil }

type fakeStore struct {
	begins, saves int
	failure       error
	seenFence     int64
}

func (f *fakeStore) Settings(context.Context) (Settings, error) {
	return Settings{Version: 1, IntervalSeconds: 60}, nil
}
func (f *fakeStore) UpdateSettings(_ context.Context, s Settings) (Settings, error) { return s, nil }
func (f *fakeStore) Begin(context.Context, time.Duration) (int64, error)            { f.begins++; return 7, nil }
func (f *fakeStore) Snapshot(context.Context) (Report, error)                       { return Report{Accounts: 2}, f.failure }
func (f *fakeStore) Save(_ context.Context, fence int64, _ Report) error {
	f.saves++
	f.seenFence = fence
	return nil
}
func TestRunOnce(t *testing.T) {
	for _, tc := range []struct {
		name                    string
		leaseErr, snapshotErr   error
		saves, begins, releases int
	}{
		{"success", nil, nil, 1, 1, 1}, {"busy", ErrBusy, nil, 0, 0, 0}, {"redis down", ErrUnavailable, nil, 0, 0, 0}, {"snapshot fails", nil, ErrUnavailable, 0, 1, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l := &fakeLease{failure: tc.leaseErr}
			s := &fakeStore{failure: tc.snapshotErr}
			service, _ := New(s, l, time.Second)
			err := service.RunOnce(context.Background())
			want := tc.leaseErr
			if want == nil {
				want = tc.snapshotErr
			}
			if !errors.Is(err, want) || s.saves != tc.saves || s.begins != tc.begins || l.releases != tc.releases {
				t.Fatalf("error=%v saves=%d begins=%d releases=%d", err, s.saves, s.begins, l.releases)
			}
			if s.saves > 0 && s.seenFence != 7 {
				t.Fatal("fence lost")
			}
		})
	}
}

type deadlineStore struct {
	fakeStore
	seen context.Context
}

func (s *deadlineStore) Settings(ctx context.Context) (Settings, error) {
	s.seen = ctx
	return Settings{}, ctx.Err()
}
func TestSettingsPreservesParentDeadlineAndValues(t *testing.T) {
	type key struct{}
	parent, cancel := context.WithDeadline(context.WithValue(context.Background(), key{}, "request"), time.Now().Add(time.Second))
	defer cancel()
	store := &deadlineStore{}
	service, _ := New(store, &fakeLease{}, time.Second)
	_, _ = service.Settings(parent)
	want, _ := parent.Deadline()
	got, ok := store.seen.Deadline()
	if !ok || !got.Equal(want) || store.seen.Value(key{}) != "request" {
		t.Fatal("context lost or deadline extended")
	}
	_, _ = service.Settings(context.Background())
	if deadline, ok := store.seen.Deadline(); !ok || time.Until(deadline) > 3*time.Second {
		t.Fatal("unbounded settings call")
	}
	cancel()
	if _, err := service.Settings(parent); !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation lost")
	}
}
