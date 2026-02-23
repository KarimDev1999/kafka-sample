package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"kafka-sample/domain"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
)

type OrderProducer struct {
	client *kgo.Client
	topic  string
}

func NewOrderProducer(brokers []string, topic string) (*OrderProducer, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.AllowAutoTopicCreation(),
		kgo.RequiredAcks(kgo.AllISRAcks()),

		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
		kgo.ProducerLinger(10 * time.Millisecond),
		kgo.ProducerBatchMaxBytes(1_000_000),

		kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelInfo, nil)),
	}

	if os.Getenv("KAFKA_USERNAME") != "" && os.Getenv("KAFKA_PASSWORD") != "" {
		opts = append(opts, kgo.SASL(plain.Auth{
			User: os.Getenv("KAFKA_USERNAME"),
			Pass: os.Getenv("KAFKA_PASSWORD"),
		}.AsMechanism()))
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka client: %w", err)
	}

	return &OrderProducer{
		client: client,
		topic:  topic,
	}, nil
}

func (p *OrderProducer) ProduceOrder(ctx context.Context, order *domain.Order) error {
	data, err := json.Marshal(order)
	if err != nil {
		return fmt.Errorf("failed to marshal order: %w", err)
	}

	record := &kgo.Record{
		Topic: p.topic,
		Key:   []byte(order.ID),
		Value: data,
		Headers: []kgo.RecordHeader{
			{Key: "event_type", Value: []byte("order_created")},
			{Key: "version", Value: []byte("1.0")},
		},
		Timestamp: time.Now(),
	}

	p.client.Produce(ctx, record, func(record *kgo.Record, err error) {
		if err != nil {
			log.Printf("Failed to produce order %s: %v", order.ID, err)
			return
		}
		log.Printf("Order %s produced to partition %d at offset %d",
			order.ID, record.Partition, record.Offset)
	})

	return nil
}

func (p *OrderProducer) Close() {
	p.client.Close()
}

func generateOrder(id int) *domain.Order {
	now := time.Now()
	return &domain.Order{
		ID:          fmt.Sprintf("order-%d", id),
		UserID:      fmt.Sprintf("user-%d", id%10),
		ProductID:   fmt.Sprintf("product-%d", id%5),
		Quantity:    id%3 + 1,
		TotalAmount: float64((id%100)+10) * 1.99,
		Status:      domain.StatusNew,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
}

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
	log.Println("Starting order producer...")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	producer, err := NewOrderProducer(
		[]string{"localhost:9092"},
		"orders",
	)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	done := make(chan struct{})

	go func() {
		sig := <-sigChan
		log.Printf("Received signal %v, initiating graceful shutdown...", sig)
		close(done)
		cancel()
	}()

	log.Println("Starting order generation...")
	orderID := 1

	for {
		select {
		case <-done:
			log.Println("Stopping order generation")
			return
		case <-time.After(2 * time.Second):
			order := generateOrder(orderID)

			produceCtx, produceCancel := context.WithTimeout(ctx, 5*time.Second)
			defer produceCancel()
			if err := producer.ProduceOrder(produceCtx, order); err != nil {
				log.Printf("Failed to produce order %d: %v", orderID, err)
			}

			orderID++
		}
	}
}
