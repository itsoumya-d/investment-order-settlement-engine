package ledger

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/itsoumya-d/investment-order-settlement-engine/pkg/models"
)

var (
	ErrInsufficientCash   = errors.New("insufficient cash balance for trade settlement")
	ErrInsufficientShares = errors.New("insufficient security shares for trade settlement")
	ErrAccountNotFound    = errors.New("account not found in ledger")
)

type AccountPosition struct {
	CashBalance     float64            // EUR currency balance
	ReservedCash    float64            // Cash held for pending limit orders
	SecurityShares  map[string]float64 // ISIN -> Fractional share count
	LastUpdatedAt   time.Time
}

type AccountLedger struct {
	mu       sync.RWMutex
	accounts map[string]*AccountPosition
}

func NewAccountLedger() *AccountLedger {
	return &AccountLedger{
		accounts: make(map[string]*AccountPosition),
	}
}

func (l *AccountLedger) CreateAccount(accountID string, initialDeposit float64) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.accounts[accountID] = &AccountPosition{
		CashBalance:    initialDeposit,
		ReservedCash:   0.0,
		SecurityShares: make(map[string]float64),
		LastUpdatedAt:  time.Now().UTC(),
	}
}

func (l *AccountLedger) GetPosition(accountID string) (AccountPosition, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	acc, ok := l.accounts[accountID]
	if !ok {
		return AccountPosition{}, ErrAccountNotFound
	}

	// Return deep copy
	shares := make(map[string]float64)
	for k, v := range acc.SecurityShares {
		shares[k] = v
	}

	return AccountPosition{
		CashBalance:    acc.CashBalance,
		ReservedCash:   acc.ReservedCash,
		SecurityShares: shares,
		LastUpdatedAt:  acc.LastUpdatedAt,
	}, nil
}

// SettleTrade atomically debits and credits accounts verifying fractional balances.
func (l *AccountLedger) SettleTrade(accountID, isin string, side models.OrderSide, quantity, executionPrice, fee float64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	acc, ok := l.accounts[accountID]
	if !ok {
		return ErrAccountNotFound
	}

	grossCost := quantity * executionPrice
	qty := models.RoundQuantity(quantity)

	if side == models.SideBuy {
		totalDebit := grossCost + fee
		if acc.CashBalance < totalDebit {
			return fmt.Errorf("%w: required %.2f EUR, available %.2f EUR", ErrInsufficientCash, totalDebit, acc.CashBalance)
		}
		acc.CashBalance -= totalDebit
		acc.SecurityShares[isin] = models.RoundQuantity(acc.SecurityShares[isin] + qty)
	} else {
		// SELL
		currentShares := acc.SecurityShares[isin]
		if currentShares < qty {
			return fmt.Errorf("%w: required %.6f shares, available %.6f shares", ErrInsufficientShares, qty, currentShares)
		}
		netCredit := grossCost - fee
		acc.SecurityShares[isin] = models.RoundQuantity(currentShares - qty)
		acc.CashBalance += netCredit
	}

	acc.LastUpdatedAt = time.Now().UTC()
	return nil
}
