package kafka

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
)

const PaymentCompletedTopic = "payment-completed"

const PaymentStatusSuccess = "SUCCESS"

const PaymentStatusFailed = "FAILED"

type PaymentCompletedEvent struct {
	EventID   string  `json:"event_id"`
	EventType string  `json:"event_type"`
	OrderID   string  `json:"order_id"`
	UserID    int     `json:"user_id"`
	PaymentID string  `json:"payment_id"`
	Status    string  `json:"status"`
	Amount    float64 `json:"amount"`
	Currency  string  `json:"currency"`
}

type Producer struct {
	client *kgo.Client
}

func brokers() []string {
	if value := os.Getenv("KAFKA_BROKERS"); value != "" {
		return strings.Split(value, ",")
	}
	return []string{"localhost:9092"}
}

func NewProducer() (*Producer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers()...),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		return nil, err
	}

	return &Producer{client: client}, nil
}

func (p *Producer) PublishPaymentCompleted(
	ctx context.Context,
	event PaymentCompletedEvent,
) error {
	value, err := json.Marshal(event)
	if err != nil {
		return err
	}

	record := &kgo.Record{
		Topic: PaymentCompletedTopic,
		Key:   []byte(event.OrderID),
		Value: value,
	}

	return p.client.ProduceSync(ctx, record).FirstErr()
}

func (p *Producer) Close() {
	p.client.Close()
	log.Println("Kafka producer closed")
}
