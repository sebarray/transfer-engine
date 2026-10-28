package transfers

import "context"

// Operations is the application contract for account and money operations.
type Operations interface {
	AccountOperations
	TransferOperations
	FundingOperations
}

// AccountOperations manages and reads owner-scoped accounts.
type AccountOperations interface {
	CreateAccount(context.Context, string, string) (Account, error)
	Account(context.Context, string, string) (Account, error)
}

// TransferOperations executes transfers between existing accounts.
type TransferOperations interface {
	Transfer(context.Context, Command) (Result, error)
}

// FundingOperations issues simulated funds under the service environment guard.
type FundingOperations interface {
	Fund(context.Context, Command) (Result, error)
}

// ReadQueries is the application contract for authorized transfer and ledger reads.
type ReadQueries interface {
	TransferQueries
	MovementQueries
}

// TransferQueries reads owner-scoped transfer details.
type TransferQueries interface {
	Transfer(context.Context, string, string) (TransferDetails, error)
}

// MovementQueries reads owner-scoped ledger pages.
type MovementQueries interface {
	Movements(context.Context, string, string, int, string) (Page, error)
}

var (
	_ Operations  = (*Service)(nil)
	_ ReadQueries = (*Queries)(nil)
)
