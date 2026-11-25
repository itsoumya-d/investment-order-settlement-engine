# Investment Order Settlement Engine

High-performance fractional investment order execution, double-entry ledger settlement, and Kafka transactional outbox engine in Go.

## Architecture
- **Fractional Order Execution**: Sub-share precision modeling (up to 6 decimal places) with limit and market execution validation.
- **Double-Entry Account Ledger**: In-memory ACID double-entry accounting verifying balance invariants between cash holdings and security inventory.
- **Kafka Transactional Outbox**: Partition-keyed event dispatching with at-least-once delivery guarantees and idempotency deduplication.

## Running Tests
```bash
go test -v ./...
```
