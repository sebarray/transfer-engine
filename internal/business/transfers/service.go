// Package transfers implements simulated financial operations through transactional ports.
package transfers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"time"
	"unicode/utf8"
)

var (
	ErrInvalidAmount     = errors.New("invalid amount")
	ErrInvalidCurrency   = errors.New("unsupported currency")
	ErrInvalidInput      = errors.New("invalid input")
	ErrSameAccount       = errors.New("same account")
	ErrNotFound          = errors.New("account not found")
	ErrUnauthorized      = errors.New("unauthorized operation")
	ErrInactive          = errors.New("inactive account")
	ErrInsufficientFunds = errors.New("insufficient funds")
	ErrOverflow          = errors.New("balance overflow")
	ErrMismatch          = errors.New("idempotency payload mismatch")
	ErrInFlight          = errors.New("operation in flight")
	ErrConflict          = errors.New("concurrency conflict")
	ErrUnavailable       = errors.New("dependency unavailable")
	ErrCommitUnknown     = errors.New("commit outcome unknown; retry with the same key")
	ErrFundingDisabled   = errors.New("test funding disabled")
)

type Account struct {
	ID       string `json:"id"`
	OwnerID  string `json:"owner_id"`
	Currency string `json:"currency"`
	Status   string `json:"status"`
	Kind     string `json:"kind"`
	Balance  int64  `json:"balance_minor"`
}
type Command struct {
	ActorID       string `json:"actor_id"`
	SourceID      string `json:"source_account_id"`
	DestinationID string `json:"destination_account_id"`
	Currency      string `json:"currency"`
	Amount        int64  `json:"amount"`
	Key           string `json:"-"`
}
type Result struct {
	ID       string `json:"id"`
	Status   string `json:"status"`
	Replayed bool   `json:"-"`
}
type Claim struct {
	Actor, Operation, Key string
	Hash                  [32]byte
}

// Tx is valid only inside WithinTransaction. Every method shares one transaction.
type Tx interface {
	Claim(context.Context, Claim) (*Result, error)
	Treasury(context.Context, string) (string, error)
	LockAccounts(context.Context, string, string) (Account, Account, error)
	Post(context.Context, Command, bool) (Result, error)
	Complete(context.Context, Claim, Result) error
}
type Store interface {
	CreateAccount(context.Context, string, string) (Account, error)
	Account(context.Context, string, string) (Account, error)
	WithinTransaction(context.Context, func(Tx) error) error
	// Resolve reads the authoritative committed result without attempting a write.
	Resolve(context.Context, Claim) (*Result, error)
}
type Service struct {
	store   Store
	funding bool
}

func New(store Store, environment string, funding bool) (*Service, error) {
	if environment != "development" && environment != "test" && environment != "production" {
		return nil, ErrInvalidInput
	}
	if funding && environment == "production" {
		return nil, ErrFundingDisabled
	}
	return &Service{store: store, funding: funding}, nil
}
func validActor(actor string) bool {
	return strings.TrimSpace(actor) != "" && len(actor) <= 200 && utf8.ValidString(actor) && !strings.ContainsRune(actor, 0)
}
func validCurrency(currency string) bool {
	return currency == "ARS" || currency == "USD" || currency == "EUR"
}
func normalizeID(id string) (string, error) {
	if len(id) != 36 || id[8] != '-' || id[13] != '-' || id[18] != '-' || id[23] != '-' {
		return "", ErrInvalidInput
	}
	compact := strings.ReplaceAll(id, "-", "")
	if len(compact) != 32 {
		return "", ErrInvalidInput
	}
	if _, err := hex.DecodeString(compact); err != nil {
		return "", ErrInvalidInput
	}
	return strings.ToLower(id), nil
}
func (s *Service) CreateAccount(ctx context.Context, actor, currency string) (Account, error) {
	if !validActor(actor) {
		return Account{}, ErrUnauthorized
	}
	if !validCurrency(currency) {
		return Account{}, ErrInvalidCurrency
	}
	return s.store.CreateAccount(ctx, actor, currency)
}
func (s *Service) Account(ctx context.Context, actor, id string) (Account, error) {
	if !validActor(actor) {
		return Account{}, ErrUnauthorized
	}
	id, err := normalizeID(id)
	if err != nil {
		return Account{}, err
	}
	return s.store.Account(ctx, actor, id)
}
func (s *Service) Transfer(ctx context.Context, c Command) (Result, error) {
	return s.execute(ctx, c, false)
}

// Fund issues fictitious money against a dedicated treasury, never an arbitrary balance update.
func (s *Service) Fund(ctx context.Context, c Command) (Result, error) {
	if !s.funding {
		return Result{}, ErrFundingDisabled
	}
	if c.SourceID != "" {
		return Result{}, ErrInvalidInput
	}
	return s.execute(ctx, c, true)
}
func (s *Service) execute(ctx context.Context, c Command, funding bool) (Result, error) {
	if !validActor(c.ActorID) {
		return Result{}, ErrUnauthorized
	}
	if c.Amount <= 0 {
		return Result{}, ErrInvalidAmount
	}
	if !validCurrency(c.Currency) {
		return Result{}, ErrInvalidCurrency
	}
	if len(c.Key) < 1 || len(c.Key) > 200 || strings.TrimSpace(c.Key) != c.Key || !utf8.ValidString(c.Key) || strings.ContainsRune(c.Key, 0) {
		return Result{}, ErrInvalidInput
	}
	var err error
	c.DestinationID, err = normalizeID(c.DestinationID)
	if err != nil {
		return Result{}, err
	}
	operation := "create_transfer"
	if funding {
		operation = "test_issuance"
	} else {
		c.SourceID, err = normalizeID(c.SourceID)
		if err != nil {
			return Result{}, err
		}
		if c.SourceID == c.DestinationID {
			return Result{}, ErrSameAccount
		}
	}
	payload, _ := json.Marshal(c)
	claim := Claim{Actor: c.ActorID, Operation: operation, Key: c.Key, Hash: sha256.Sum256(payload)}
	var result Result
	err = s.store.WithinTransaction(ctx, func(tx Tx) error {
		replay, err := tx.Claim(ctx, claim)
		if err != nil {
			return err
		}
		if replay != nil {
			result = *replay
			result.Replayed = true
			return nil
		}
		if funding {
			c.SourceID, err = tx.Treasury(ctx, c.Currency)
			if err != nil {
				return err
			}
		}
		source, target, err := tx.LockAccounts(ctx, c.SourceID, c.DestinationID)
		if err != nil {
			return err
		}
		if err = ValidateMovement(c, source, target, funding); err != nil {
			return err
		}
		result, err = tx.Post(ctx, c, funding)
		if err != nil {
			return err
		}
		return tx.Complete(ctx, claim, result)
	})
	if errors.Is(err, ErrCommitUnknown) {
		// A canceled HTTP request must not prevent bounded reconciliation of an
		// already attempted commit. Never repeat the financial write here.
		recoveryCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer cancel()
		persisted, resolveErr := s.store.Resolve(recoveryCtx, claim)
		if resolveErr == nil && persisted != nil {
			persisted.Replayed = true
			return *persisted, nil
		}
		if errors.Is(resolveErr, ErrMismatch) {
			return Result{}, ErrMismatch
		}
		// Absence is not proof of rollback: the original backend may still be
		// completing COMMIT. The unique key guards a subsequent caller retry.
		return Result{}, ErrCommitUnknown
	}
	if err != nil {
		return Result{}, err
	}
	return result, nil
}

// ValidateMovement must run on the locked account snapshots before any mutation.
func ValidateMovement(c Command, source, target Account, funding bool) error {
	if c.Amount <= 0 {
		return ErrInvalidAmount
	}
	if source.ID == target.ID {
		return ErrSameAccount
	}
	if target.Kind != "customer" || (!funding && source.Kind != "customer") || (funding && source.Kind != "test_treasury") {
		return ErrNotFound
	}
	if (!funding && source.OwnerID != c.ActorID) || (funding && target.OwnerID != c.ActorID) {
		return ErrNotFound
	}
	if source.Status != "active" || target.Status != "active" {
		return ErrInactive
	}
	if !validCurrency(c.Currency) || source.Currency != c.Currency || target.Currency != c.Currency {
		return ErrInvalidCurrency
	}
	if !funding && source.Balance < c.Amount {
		return ErrInsufficientFunds
	}
	if target.Balance > math.MaxInt64-c.Amount || source.Balance < math.MinInt64+c.Amount {
		return ErrOverflow
	}
	return nil
}
