package models

import (
	"errors"
	"math"
	"time"
)

type OrderSide string

const (
	SideBuy  OrderSide = "BUY"
	SideSell OrderSide = "SELL"
)

type OrderType string

const (
	TypeMarket OrderType = "MARKET"
	TypeLimit  OrderType = "LIMIT"
)

type OrderStatus string

const (
	StatusPending  OrderStatus = "PENDING"
	StatusFilled   OrderStatus = "FILLED"
	StatusRejected OrderStatus = "REJECTED"
	StatusSettled  OrderStatus = "SETTLED"
)

// Order represents an investment order supporting fractional shares up to 6 decimal places.
type Order struct {
	ID             string      `json:"id"`
	AccountID      string      `json:"account_id"`
	InstrumentISIN string      `json:"instrument_isin"`
	Side           OrderSide   `json:"side"`
	OrderType      OrderType   `json:"order_type"`
	Quantity       float64     `json:"quantity"` // Fractional shares
	LimitPrice     float64     `json:"limit_price,omitempty"`
	ExecutionPrice float64     `json:"execution_price,omitempty"`
	Status         OrderStatus `json:"status"`
	CreatedAt      time.Time   `json:"created_at"`
	SettledAt      *time.Time  `json:"settled_at,omitempty"`
}

func (o *Order) Validate() error {
	if o.ID == "" {
		return errors.New("order ID is required")
	}
	if o.AccountID == "" {
		return errors.New("account ID is required")
	}
	if len(o.InstrumentISIN) != 12 {
		return errors.New("instrument ISIN must be exactly 12 characters")
	}
	if o.Quantity <= 0.000001 {
		return errors.New("quantity must be greater than 0.000001 shares")
	}
	if o.Side != SideBuy && o.Side != SideSell {
		return errors.New("invalid order side")
	}
	if o.OrderType == TypeLimit && o.LimitPrice <= 0 {
		return errors.New("limit price must be strictly positive for limit orders")
	}
	return nil
}

// RoundQuantity normalizes float64 to 6 decimal precision for fractional investment accuracy.
func RoundQuantity(val float64) float64 {
	return math.Round(val*1000000) / 1000000
}
