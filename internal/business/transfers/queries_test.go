package transfers

import (
	"context"
	"testing"
)

type queryReader struct {
	calls  int
	before int64
	limit  int
}

func (r *queryReader) TransferDetails(context.Context, string, string) (TransferDetails, error) {
	r.calls++
	return TransferDetails{}, nil
}
func (r *queryReader) Movements(_ context.Context, _, _ string, limit int, before int64) ([]Movement, error) {
	r.calls++
	r.before = before
	r.limit = limit
	return []Movement{{ID: 5}, {ID: 4}}, nil
}
func TestQueryPaginationBoundary(t *testing.T) {
	r := &queryReader{}
	q := NewQueries(r)
	id := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	page, err := q.Movements(context.Background(), "alice", id, 1, "")
	if err != nil || len(page.Items) != 1 || page.NextCursor == "" || r.limit != 2 {
		t.Fatal("missing lookahead pagination")
	}
	if _, err = q.Movements(context.Background(), "alice", id, 1, page.NextCursor); err != nil || r.before != 5 {
		t.Fatal("cursor not restored")
	}
	for _, cursor := range []string{"invalid!", "MA", "LTE", "OTIyMzM3MjAzNjg1NDc3NTgwOA"} {
		before := r.calls
		if _, err = q.Movements(context.Background(), "alice", id, 1, cursor); err == nil || r.calls != before {
			t.Fatal("invalid cursor reached store")
		}
	}
	for _, limit := range []int{0, 101} {
		if _, err = q.Movements(context.Background(), "alice", id, limit, ""); err == nil {
			t.Fatal("invalid limit")
		}
	}
	if _, err = q.Transfer(context.Background(), "", id); err == nil {
		t.Fatal("actor missing")
	}
}
