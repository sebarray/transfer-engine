package httpapi

import (
	"time"

	"transfer-engine/internal/business/transfers"
)

type createAccountRequest struct {
	Currency string `json:"currency"`
}

type createTransferRequest struct {
	Source      string `json:"source_account_id"`
	Destination string `json:"destination_account_id"`
	Currency    string `json:"currency"`
	Amount      int64  `json:"amount"`
}

type fundRequest struct {
	Destination string `json:"destination_account_id"`
	Currency    string `json:"currency"`
	Amount      int64  `json:"amount"`
}

type accountResponse struct {
	ID       string `json:"id"`
	OwnerID  string `json:"owner_id"`
	Currency string `json:"currency"`
	Status   string `json:"status"`
	Kind     string `json:"kind"`
	Balance  int64  `json:"balance_minor"`
}

type balanceResponse struct {
	ID       string `json:"account_id"`
	Currency string `json:"currency"`
	Balance  int64  `json:"balance_minor"`
}

type movementResultResponse struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

type transferDetailsResponse struct {
	ID            string    `json:"id"`
	SourceID      string    `json:"source_account_id"`
	DestinationID string    `json:"destination_account_id"`
	Currency      string    `json:"currency"`
	Amount        int64     `json:"amount"`
	Status        string    `json:"status"`
	CreatedAt     time.Time `json:"created_at"`
}

type movementResponse struct {
	ID        int64     `json:"id"`
	JournalID string    `json:"journal_id"`
	Currency  string    `json:"currency"`
	Amount    int64     `json:"amount_minor"`
	CreatedAt time.Time `json:"created_at"`
}

type movementsResponse struct {
	Items      []movementResponse `json:"items"`
	NextCursor string             `json:"next_cursor,omitempty"`
}

func accountDTO(v transfers.Account) accountResponse {
	return accountResponse{ID: v.ID, OwnerID: v.OwnerID, Currency: v.Currency, Status: v.Status, Kind: v.Kind, Balance: v.Balance}
}

func transferDetailsDTO(v transfers.TransferDetails) transferDetailsResponse {
	return transferDetailsResponse{ID: v.ID, SourceID: v.SourceID, DestinationID: v.DestinationID, Currency: v.Currency, Amount: v.Amount, Status: v.Status, CreatedAt: v.CreatedAt}
}

func movementsDTO(v transfers.Page) movementsResponse {
	items := make([]movementResponse, len(v.Items))
	for i, item := range v.Items {
		items[i] = movementResponse{ID: item.ID, JournalID: item.JournalID, Currency: item.Currency, Amount: item.Amount, CreatedAt: item.CreatedAt}
	}
	return movementsResponse{Items: items, NextCursor: v.NextCursor}
}
