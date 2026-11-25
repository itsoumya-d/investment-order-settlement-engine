package events

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

type OutboxEvent struct {
	EventID     string          `json:"event_id"`
	Topic       string          `json:"topic"`
	Key         string          `json:"key"` // Partition key (e.g. AccountID or ISIN)
	Payload     json.RawMessage `json:"payload"`
	CreatedAt   time.Time       `json:"created_at"`
	PublishedAt *time.Time      `json:"published_at,omitempty"`
}

type KafkaOutbox struct {
	mu        sync.RWMutex
	events    []*OutboxEvent
	published map[string]bool // Idempotency tracker
}

func NewKafkaOutbox() *KafkaOutbox {
	return &KafkaOutbox{
		events:    make([]*OutboxEvent, 0),
		published: make(map[string]bool),
	}
}

func (o *KafkaOutbox) Enqueue(topic, key string, payload interface{}) (*OutboxEvent, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal event payload: %w", err)
	}

	evtID := fmt.Sprintf("evt_%d_%d", time.Now().UnixNano(), len(o.events)+1)
	evt := &OutboxEvent{
		EventID:   evtID,
		Topic:     topic,
		Key:       key,
		Payload:   raw,
		CreatedAt: time.Now().UTC(),
	}

	o.events = append(o.events, evt)
	return evt, nil
}

func (o *KafkaOutbox) DispatchPending() int {
	o.mu.Lock()
	defer o.mu.Unlock()

	dispatched := 0
	now := time.Now().UTC()
	for _, evt := range o.events {
		if evt.PublishedAt == nil && !o.published[evt.EventID] {
			evt.PublishedAt = &now
			o.published[evt.EventID] = true
			dispatched++
		}
	}
	return dispatched
}

func (o *KafkaOutbox) GetEventsForTopic(topic string) []*OutboxEvent {
	o.mu.RLock()
	defer o.mu.RUnlock()

	var res []*OutboxEvent
	for _, e := range o.events {
		if e.Topic == topic {
			res = append(res, e)
		}
	}
	return res
}
