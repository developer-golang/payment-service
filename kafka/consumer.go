package kafka

import (
	"context"
	"encoding/json"
	"log"

	"github.com/twmb/franz-go/pkg/kgo"
)

const OrderCreatedTopic = "order-created"

type OrderCreatedEvent struct {
	EventID   string  `json:"event_id"`
	EventType string  `json:"event_type"`
	OrderID   string  `json:"order_id"`
	UserID    int     `json:"user_id"`
	Amount    float64 `json:"amount"`
	Currency  string  `json:"currency"`
}

func NewConsumer() (*kgo.Client, error) {
	return kgo.NewClient(
		kgo.SeedBrokers(brokers()...),
		kgo.ConsumerGroup("payment-service-group"),
		kgo.ConsumeTopics(OrderCreatedTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.AllowAutoTopicCreation(),
		kgo.DisableAutoCommit(),
	)
}

func ConsumeOrders(
	ctx context.Context,
	client *kgo.Client,
	handle func(context.Context, OrderCreatedEvent) error,
) {
	for {
		fetches := client.PollFetches(ctx)

		if fetches.IsClientClosed() || ctx.Err() != nil {
			return
		}

		fetches.EachError(func(topic string, partition int32, err error) {
			log.Printf("Kafka error: topic=%s partition=%d err=%v",
				topic, partition, err)
		})

		fetches.EachRecord(func(record *kgo.Record) {
			var event OrderCreatedEvent

			if err := json.Unmarshal(record.Value, &event); err != nil {
				log.Printf("Invalid order event: %v", err)
				if err := client.CommitRecords(ctx, record); err != nil {
					log.Printf("Commit failed: %v", err)
				}
				return
			}

			log.Printf(
				"Processing payment for order: ID=%s Amount=%.2f",
				event.OrderID,
				event.Amount,
			)

			if err := handle(ctx, event); err != nil {
				log.Printf("Failed to process payment for order %s: %v", event.OrderID, err)
				return
			}

			if err := client.CommitRecords(ctx, record); err != nil {
				log.Printf("Commit failed: %v", err)
			}
		})
	}
}
