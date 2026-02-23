package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"kafka-sample/domain"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
)

type OrderProcessor struct {
	client      *kgo.Client
	topic       string
	groupID     string
	workers     int
	processTime time.Duration
}

func NewOrderProcessor(brokers []string, topic, groupID string, workers int) (*OrderProcessor, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),

		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(topic),

		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),

		kgo.DisableAutoCommit(),

		kgo.SessionTimeout(30 * time.Second),
		kgo.RebalanceTimeout(30 * time.Second),

		kgo.FetchMaxBytes(50_000_000),

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

	return &OrderProcessor{
		client:      client,
		topic:       topic,
		groupID:     groupID,
		workers:     workers,
		processTime: 100 * time.Millisecond,
	}, nil
}

type OrderProcessingTask struct {
	record *kgo.Record
	order  *domain.Order
}

func (p *OrderProcessor) processOrder(ctx context.Context, task *OrderProcessingTask) error {
	log.Printf("🔄 Processing order %s (partition: %d, offset: %d)",
		task.order.ID, task.record.Partition, task.record.Offset)

	select {
	case <-time.After(p.processTime):
		task.order.Status = domain.StatusProcessing
		time.Sleep(p.processTime)
		task.order.Status = domain.StatusCompleted
		task.order.UpdatedAt = time.Now()

		log.Printf("Order %s processed successfully", task.order.ID)
		return nil

	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *OrderProcessor) Run(ctx context.Context) error {
	log.Printf("Starting order processor with %d workers", p.workers)

	tasks := make(chan *OrderProcessingTask, p.workers*2)
	results := make(chan *kgo.Record, p.workers*2)
	errors := make(chan error, p.workers*2)

	var wg sync.WaitGroup

	for i := 0; i < p.workers; i++ {
		wg.Add(1)
		go p.worker(ctx, i, tasks, results, errors, &wg)
	}

	go func() {
		var committed []*kgo.Record
		commitTicker := time.NewTicker(5 * time.Second)
		defer commitTicker.Stop()

		for {
			select {
			case <-ctx.Done():
				if len(committed) > 0 {
					p.commitRecords(ctx, committed)
				}
				return

			case record := <-results:
				committed = append(committed, record)

			case <-commitTicker.C:
				if len(committed) > 0 {
					p.commitRecords(ctx, committed)
					committed = nil
				}
			}
		}
	}()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case err := <-errors:
				if err != nil {
					log.Printf("Processing error: %v", err)
				}
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			log.Println("Received shutdown signal, waiting for workers to finish...")
			close(tasks)
			wg.Wait()
			log.Println("All workers finished")
			return nil

		default:
			fetches := p.client.PollFetches(ctx)
			if fetches.IsClientClosed() {
				return nil
			}

			fetches.EachError(func(topic string, partition int32, err error) {
				log.Printf("Fetch error: topic=%s partition=%d err=%v", topic, partition, err)
			})

			fetches.EachRecord(func(record *kgo.Record) {
				var order domain.Order
				if err := json.Unmarshal(record.Value, &order); err != nil {
					log.Printf("Failed to unmarshal order: %v", err)
					return
				}

				select {
				case tasks <- &OrderProcessingTask{
					record: record,
					order:  &order,
				}:
				case <-ctx.Done():
					return
				}
			})
		}
	}
}

func (p *OrderProcessor) worker(ctx context.Context, id int,
	tasks <-chan *OrderProcessingTask,
	results chan<- *kgo.Record,
	errors chan<- error,
	wg *sync.WaitGroup) {

	defer wg.Done()
	log.Printf("Worker %d started", id)

	for {
		select {
		case <-ctx.Done():
			log.Printf("Worker %d stopping", id)
			return

		case task, ok := <-tasks:
			if !ok {
				log.Printf("Worker %d: tasks channel closed", id)
				return
			}

			taskCtx, taskCancel := context.WithTimeout(ctx, 30*time.Second)

			if err := p.processOrder(taskCtx, task); err != nil {
				errors <- fmt.Errorf("worker %d: %w", id, err)
			} else {
				select {
				case results <- task.record:
				case <-ctx.Done():
				}
			}

			taskCancel()
		}
	}
}

func (p *OrderProcessor) commitRecords(ctx context.Context, records []*kgo.Record) {
	if len(records) == 0 {
		return
	}

	log.Printf("Committing %d records", len(records))

	err := p.client.CommitRecords(ctx, records...)
	if err != nil {
		log.Printf("Failed to commit records: %v", err)
	}
}

func (p *OrderProcessor) Close() {
	p.client.Close()
}

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
	log.Println("Starting order consumer...")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	processor, err := NewOrderProcessor(
		[]string{"localhost:9092"},
		"orders",
		"order-processor-group",
		5,
	)
	if err != nil {
		log.Fatalf("Failed to create order processor: %v", err)
	}
	defer processor.Close()

	processorErr := make(chan error, 1)
	go func() {
		processorErr <- processor.Run(ctx)
	}()

	select {
	case sig := <-sigChan:
		log.Printf("Received signal %v, initiating graceful shutdown...", sig)
		cancel()

		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer shutdownCancel()

		select {
		case <-shutdownCtx.Done():
			log.Println("Shutdown timeout exceeded")
		case err := <-processorErr:
			if err != nil {
				log.Printf("Processor error during shutdown: %v", err)
			}
		}

	case err := <-processorErr:
		if err != nil {
			log.Fatalf("Processor error: %v", err)
		}
	}

	log.Println("Consumer shutdown complete")
}
