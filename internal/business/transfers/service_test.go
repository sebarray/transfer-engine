package transfers

import (
	"context"
	"errors"
	"math"
	"testing"
)

type fakeStore struct {
	tx      fakeTx
	commits int
}

func (f *fakeStore) CreateAccount(context.Context, string, string) (Account, error) {
	return Account{}, nil
}
func (f *fakeStore) Account(context.Context, string, string) (Account, error) { return Account{}, nil }
func (f *fakeStore) Resolve(context.Context, Claim) (*Result, error) {
	return f.tx.replay, f.tx.claimErr
}
func (f *fakeStore) WithinTransaction(_ context.Context, fn func(Tx) error) error {
	err := fn(&f.tx)
	if err == nil {
		f.commits++
	}
	return err
}

type fakeTx struct {
	source, target    Account
	replay            *Result
	claimErr, postErr error
	posts             int
	completed         bool
}

func (f *fakeTx) Claim(context.Context, Claim) (*Result, error)    { return f.replay, f.claimErr }
func (f *fakeTx) Treasury(context.Context, string) (string, error) { return f.source.ID, nil }
func (f *fakeTx) LockAccounts(context.Context, string, string) (Account, Account, error) {
	return f.source, f.target, nil
}
func (f *fakeTx) Post(context.Context, Command, bool) (Result, error) {
	f.posts++
	return Result{ID: "result", Status: "completed"}, f.postErr
}
func (f *fakeTx) Complete(context.Context, Claim, Result) error { f.completed = true; return nil }
func TestTransferDecisions(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*Command, *fakeTx)
		want   error
	}{
		{"success", func(*Command, *fakeTx) {}, nil},
		{"amount", func(c *Command, _ *fakeTx) { c.Amount = 0 }, ErrInvalidAmount},
		{"currency", func(c *Command, _ *fakeTx) { c.Currency = "ZZZ" }, ErrInvalidCurrency},
		{"same", func(c *Command, _ *fakeTx) { c.DestinationID = c.SourceID }, ErrSameAccount},
		{"ownership", func(_ *Command, f *fakeTx) { f.source.OwnerID = "mallory" }, ErrNotFound},
		{"inactive", func(_ *Command, f *fakeTx) { f.target.Status = "closed" }, ErrInactive},
		{"funds", func(_ *Command, f *fakeTx) { f.source.Balance = 0 }, ErrInsufficientFunds},
		{"overflow", func(_ *Command, f *fakeTx) { f.target.Balance = math.MaxInt64 }, ErrOverflow},
		{"mismatch", func(_ *Command, f *fakeTx) { f.claimErr = ErrMismatch }, ErrMismatch},
		{"rollback", func(_ *Command, f *fakeTx) { f.postErr = ErrUnavailable }, ErrUnavailable},
		{"replay", func(_ *Command, f *fakeTx) { f.replay = &Result{ID: "existing", Status: "completed"} }, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := Command{ActorID: "alice", SourceID: "00000000-0000-0000-0000-000000000001", DestinationID: "00000000-0000-0000-0000-000000000002", Currency: "ARS", Amount: 10, Key: "key"}
			f := &fakeStore{tx: fakeTx{source: Account{ID: c.SourceID, OwnerID: "alice", Kind: "customer", Status: "active", Currency: "ARS", Balance: 100}, target: Account{ID: c.DestinationID, Kind: "customer", Status: "active", Currency: "ARS"}}}
			tc.change(&c, &f.tx)
			s, _ := New(f, "test", true)
			r, err := s.Transfer(context.Background(), c)
			if !errors.Is(err, tc.want) {
				t.Fatalf("got %v want %v", err, tc.want)
			}
			if err != nil && f.commits != 0 {
				t.Fatal("failed operation committed")
			}
			if tc.name == "success" && (!f.tx.completed || f.tx.posts != 1) {
				t.Fatal("missing persisted result")
			}
			if tc.name == "replay" && (!r.Replayed || r.ID != "existing" || f.tx.posts != 0) {
				t.Fatal("replay posted a movement")
			}
		})
	}
}
func TestFundingGuard(t *testing.T) {
	if _, err := New(&fakeStore{}, "production", true); !errors.Is(err, ErrFundingDisabled) {
		t.Fatal(err)
	}
	s, _ := New(&fakeStore{}, "test", false)
	if _, err := s.Fund(context.Background(), Command{}); !errors.Is(err, ErrFundingDisabled) {
		t.Fatal(err)
	}
}
