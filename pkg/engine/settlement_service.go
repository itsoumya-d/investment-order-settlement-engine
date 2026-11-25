package engine

import (
	"time"

	"github.com/itsoumya-d/investment-order-settlement-engine/pkg/events"
	"github.com/itsoumya-d/investment-order-settlement-engine/pkg/ledger"
	"github.com/itsoumya-d/investment-order-settlement-engine/pkg/models"
)

type SettlementResult struct {
	Order   *models.Order
	Success bool
	Error   error
	EventID string
}

type SettlementService struct {
	ledger *ledger.AccountLedger
	outbox *events.KafkaOutbox
	feeBps float64 // Fee in basis points (e.g. 10 bps = 0.10%)
}

func NewSettlementService(l *ledger.AccountLedger, outbox *events.KafkaOutbox, feeBps float64) *SettlementService {
	return &SettlementService{
		ledger: l,
		outbox: outbox,
		feeBps: feeBps,
	}
}

func (s *SettlementService) CalculateFee(grossAmount float64) float64 {
	return (grossAmount * s.feeBps) / 10000.0
}

func (s *SettlementService) ProcessOrderExecution(order *models.Order, executionPrice float64) SettlementResult {
	if err := order.Validate(); err != nil {
		order.Status = models.StatusRejected
		return SettlementResult{Order: order, Success: false, Error: err}
	}

	order.ExecutionPrice = executionPrice
	grossAmount := order.Quantity * executionPrice
	fee := s.CalculateFee(grossAmount)

	err := s.ledger.SettleTrade(order.AccountID, order.InstrumentISIN, order.Side, order.Quantity, executionPrice, fee)
	if err != nil {
		order.Status = models.StatusRejected
		// Publish rejection event to Kafka outbox
		rejEvt, _ := s.outbox.Enqueue("orders.rejected", order.AccountID, map[string]interface{}{
			"order_id": order.ID,
			"account_id": order.AccountID,
			"isin": order.InstrumentISIN,
			"reason": err.Error(),
			"timestamp": time.Now().UTC(),
		})
		s.outbox.DispatchPending()

		var evtID string
		if rejEvt != nil {
			evtID = rejEvt.EventID
		}
		return SettlementResult{Order: order, Success: false, Error: err, EventID: evtID}
	}

	now := time.Now().UTC()
	order.Status = models.StatusSettled
	order.SettledAt = &now

	// Publish settled event to Kafka outbox
	settledEvt, _ := s.outbox.Enqueue("orders.settled", order.InstrumentISIN, map[string]interface{}{
		"order_id": order.ID,
		"account_id": order.AccountID,
		"isin": order.InstrumentISIN,
		"side": order.Side,
		"quantity": order.Quantity,
		"execution_price": executionPrice,
		"gross_amount": grossAmount,
		"fee": fee,
		"settled_at": now,
	})
	s.outbox.DispatchPending()

	evtID := ""
	if settledEvt != nil {
		evtID = settledEvt.EventID
	}

	return SettlementResult{
		Order:   order,
		Success: true,
		EventID: evtID,
	}
}
