package transfers

import (
	"context"
	"encoding/base64"
	"strconv"
	"time"
)

type TransferDetails struct {
	ID            string    `json:"id"`
	SourceID      string    `json:"source_account_id"`
	DestinationID string    `json:"destination_account_id"`
	Currency      string    `json:"currency"`
	Amount        int64     `json:"amount"`
	Status        string    `json:"status"`
	CreatedAt     time.Time `json:"created_at"`
}
type Movement struct {
	ID        int64     `json:"id"`
	JournalID string    `json:"journal_id"`
	Currency  string    `json:"currency"`
	Amount    int64     `json:"amount_minor"`
	CreatedAt time.Time `json:"created_at"`
}
type Page struct {
	Items      []Movement `json:"items"`
	NextCursor string     `json:"next_cursor,omitempty"`
}
type Reader interface {
	TransferDetails(context.Context, string, string) (TransferDetails, error)
	Movements(context.Context, string, string, int, int64) ([]Movement, error)
}
type Queries struct{ reader Reader }

func NewQueries(reader Reader) *Queries { return &Queries{reader: reader} }
func (q *Queries) Transfer(ctx context.Context, actor, id string) (TransferDetails, error) {
	if !validActor(actor) {
		return TransferDetails{}, ErrUnauthorized
	}
	id, err := normalizeID(id)
	if err != nil {
		return TransferDetails{}, err
	}
	return q.reader.TransferDetails(ctx, actor, id)
}
func (q *Queries) Movements(ctx context.Context, actor, id string, limit int, cursor string) (Page, error) {
	if !validActor(actor) {
		return Page{}, ErrUnauthorized
	}
	id, err := normalizeID(id)
	if err != nil {
		return Page{}, err
	}
	if limit < 1 || limit > 100 || len(cursor) > 40 {
		return Page{}, ErrInvalidInput
	}
	var before int64
	if cursor != "" {
		b, e := base64.RawURLEncoding.DecodeString(cursor)
		if e != nil {
			return Page{}, ErrInvalidInput
		}
		before, e = strconv.ParseInt(string(b), 10, 64)
		if e != nil || before < 1 {
			return Page{}, ErrInvalidInput
		}
	}
	items, err := q.reader.Movements(ctx, actor, id, limit+1, before)
	if err != nil {
		return Page{}, err
	}
	page := Page{Items: items}
	if page.Items == nil {
		page.Items = []Movement{}
	}
	if len(items) > limit {
		page.Items = items[:limit]
		page.NextCursor = base64.RawURLEncoding.EncodeToString([]byte(strconv.FormatInt(items[limit-1].ID, 10)))
	}
	return page, nil
}
