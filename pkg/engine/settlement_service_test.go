package engine_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/itsoumya-d/investment-order-settlement-engine/pkg/engine"
	"github.com/itsoumya-d/investment-order-settlement-engine/pkg/events"
	"github.com/itsoumya-d/investment-order-settlement-engine/pkg/ledger"
	"github.com/itsoumya-d/investment-order-settlement-engine/pkg/models"
)

func setupTestService() (*engine.SettlementService, *ledger.AccountLedger, *events.KafkaOutbox) {
	accLedger := ledger.NewAccountLedger()
	outbox := events.NewKafkaOutbox()
	// 10 basis points fee (0.10%)
	service := engine.NewSettlementService(accLedger, outbox, 10.0)
	return service, accLedger, outbox
}

func TestOrderValidation(t *testing.T) {
	order := &models.Order{
		ID:             "ord_001",
		AccountID:      "acc_123",
		InstrumentISIN: "US0378331005", // Apple
		Side:           models.SideBuy,
		OrderType:      models.TypeMarket,
		Quantity:       1.5,
	}
	if err := order.Validate(); err != nil {
		t.Fatalf("expected valid order, got error: %v", err)
	}

	// Invalid ISIN length
	badISIN := *order
	badISIN.InstrumentISIN = "INVALID"
	if err := badISIN.Validate(); err == nil {
		t.Fatal("expected error on invalid ISIN, got nil")
	}

	// Invalid Quantity
	badQty := *order
	badQty.Quantity = 0.0000001
	if err := badQty.Validate(); err == nil {
		t.Fatal("expected error on zero/low quantity, got nil")
	}
}

func TestFractionalBuyOrderSettlement(t *testing.T) {
	service, accLedger, outbox := setupTestService()
	accountID := "acc_buyer_01"
	isin := "US0378331005"

	// Deposit 1,000 EUR
	accLedger.CreateAccount(accountID, 1000.0)

	// Buy 2.5 shares at 150.0 EUR (Gross = 375.0 EUR, Fee = 0.375 EUR, Total = 375.375 EUR)
	order := &models.Order{
		ID:             "ord_buy_1",
		AccountID:      accountID,
		InstrumentISIN: isin,
		Side:           models.SideBuy,
		OrderType:      models.TypeMarket,
		Quantity:       2.5,
		CreatedAt:      time.Now().UTC(),
	}

	res := service.ProcessOrderExecution(order, 150.0)
	if !res.Success {
		t.Fatalf("expected settlement success, got error: %v", res.Error)
	}
	if res.Order.Status != models.StatusSettled {
		t.Fatalf("expected status SETTLED, got %s", res.Order.Status)
	}

	pos, err := accLedger.GetPosition(accountID)
	if err != nil {
		t.Fatalf("failed to get position: %v", err)
	}

	expectedCash := 1000.0 - 375.375
	if pos.CashBalance != expectedCash {
		t.Fatalf("expected cash %.3f, got %.3f", expectedCash, pos.CashBalance)
	}
	if pos.SecurityShares[isin] != 2.5 {
		t.Fatalf("expected 2.5 shares, got %.6f", pos.SecurityShares[isin])
	}

	// Verify Kafka outbox event
	settledEvents := outbox.GetEventsForTopic("orders.settled")
	if len(settledEvents) != 1 {
		t.Fatalf("expected 1 settled event in outbox, got %d", len(settledEvents))
	}
}

func TestInsufficientCashRejection(t *testing.T) {
	service, accLedger, outbox := setupTestService()
	accountID := "acc_low_funds"
	isin := "US0378331005"

	// Deposit only 50 EUR
	accLedger.CreateAccount(accountID, 50.0)

	order := &models.Order{
		ID:             "ord_buy_fail",
		AccountID:      accountID,
		InstrumentISIN: isin,
		Side:           models.SideBuy,
		OrderType:      models.TypeMarket,
		Quantity:       1.0,
		CreatedAt:      time.Now().UTC(),
	}

	// Execution price 200 EUR exceeds balance
	res := service.ProcessOrderExecution(order, 200.0)
	if res.Success {
		t.Fatal("expected failure due to insufficient funds, but succeeded")
	}
	if res.Order.Status != models.StatusRejected {
		t.Fatalf("expected status REJECTED, got %s", res.Order.Status)
	}

	// Verify rejected event in outbox
	rejEvents := outbox.GetEventsForTopic("orders.rejected")
	if len(rejEvents) != 1 {
		t.Fatalf("expected 1 rejected event in outbox, got %d", len(rejEvents))
	}
}

func TestFractionalSellOrderSettlement(t *testing.T) {
	service, accLedger, _ := setupTestService()
	accountID := "acc_seller_01"
	isin := "US02079K3059" // Alphabet

	accLedger.CreateAccount(accountID, 100.0)

	// Buy 1.75 shares
	buyOrder := &models.Order{
		ID:             "ord_buy_init",
		AccountID:      accountID,
		InstrumentISIN: isin,
		Side:           models.SideBuy,
		OrderType:      models.TypeMarket,
		Quantity:       0.5,
		CreatedAt:      time.Now().UTC(),
	}
	service.ProcessOrderExecution(buyOrder, 100.0) // Cost = 50 + 0.05 = 50.05 EUR

	// Attempt to sell 1.0 shares (insufficient, only holds 0.5)
	failSell := &models.Order{
		ID:             "ord_sell_fail",
		AccountID:      accountID,
		InstrumentISIN: isin,
		Side:           models.SideSell,
		OrderType:      models.TypeMarket,
		Quantity:       1.0,
		CreatedAt:      time.Now().UTC(),
	}
	resFail := service.ProcessOrderExecution(failSell, 110.0)
	if resFail.Success {
		t.Fatal("expected sell failure due to insufficient shares, but succeeded")
	}

	// Sell 0.25 fractional shares
	sellOrder := &models.Order{
		ID:             "ord_sell_ok",
		AccountID:      accountID,
		InstrumentISIN: isin,
		Side:           models.SideSell,
		OrderType:      models.TypeMarket,
		Quantity:       0.25,
		CreatedAt:      time.Now().UTC(),
	}
	resOk := service.ProcessOrderExecution(sellOrder, 120.0)
	if !resOk.Success {
		t.Fatalf("expected sell success, got error: %v", resOk.Error)
	}

	pos, _ := accLedger.GetPosition(accountID)
	if pos.SecurityShares[isin] != 0.25 {
		t.Fatalf("expected 0.25 remaining shares, got %.6f", pos.SecurityShares[isin])
	}
}

func TestConcurrentSettlementsRaceSafety(t *testing.T) {
	service, accLedger, _ := setupTestService()
	accountID := "acc_concurrent"
	isin := "US5949181045" // Microsoft

	// Deposit 10,000 EUR
	accLedger.CreateAccount(accountID, 10000.0)

	var wg sync.WaitGroup
	workers := 20
	ordersPerWorker := 10

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < ordersPerWorker; i++ {
				order := &models.Order{
					ID:             fmt.Sprintf("ord_%d_%d", workerID, i),
					AccountID:      accountID,
					InstrumentISIN: isin,
					Side:           models.SideBuy,
					OrderType:      models.TypeMarket,
					Quantity:       0.05, // 0.05 * 200 = 10 EUR
					CreatedAt:      time.Now().UTC(),
				}
				service.ProcessOrderExecution(order, 200.0)
			}
		}(w)
	}

	wg.Wait()

	pos, err := accLedger.GetPosition(accountID)
	if err != nil {
		t.Fatalf("failed to get position: %v", err)
	}

	expectedTotalShares := float64(workers*ordersPerWorker) * 0.05
	if models.RoundQuantity(pos.SecurityShares[isin]) != models.RoundQuantity(expectedTotalShares) {
		t.Fatalf("expected %.6f shares after concurrent buys, got %.6f", expectedTotalShares, pos.SecurityShares[isin])
	}
}
