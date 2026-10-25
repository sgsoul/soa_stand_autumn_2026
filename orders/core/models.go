package core

import (
	"errors"
	"time"
)

var (
	ErrNotFound = errors.New("not found")
	ErrInvalid  = errors.New("invalid input")
)

const (
	Pending  = "pending"
	Paid     = "paid"
	Rejected = "rejected"

	ReservePhase = "reserve"
	PaymentPhase = "payment"
	ReleasePhase = "release"
)

type Item struct {
	SKU        string `json:"sku"`
	Qty        int64  `json:"qty"`
	PriceCents int64  `json:"price_cents"`
}

type OrderRequest struct {
	UserID string `json:"user_id"`
	Items  []Item `json:"items"`
}

type Order struct {
	ID          string `json:"id"`
	Status      string `json:"status"`
	AmountCents int64  `json:"amount_cents"`

	UserID string `json:"-"`
	Items  []Item `json:"-"`
	Phase  string `json:"-"`
}

type OrderPaidEvent struct {
	ID         string    `json:"id"`
	Type       string    `json:"type"`
	OrderID    string    `json:"order_id"`
	OccurredAt time.Time `json:"occurred_at"`
}

type DeliveryEvent struct {
	ID          string `json:"id"`
	Type        string `json:"type"`
	UserID      string `json:"user_id"`
	BonusPoints int64  `json:"bonus_points"`
}