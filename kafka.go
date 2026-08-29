package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

const (
	topicRidershipRaw   = "ridership.events.raw"
	topicRouteRecommend = "routing.recommendations"
	topicAlerts         = "routing.alerts"
	consumerGroupID     = "dptrb-routing-engine"
)

// KafkaClients bundles the reader/writer handles used by the
// routing engine.
type KafkaClients struct {
	Reader      *kafka.Reader
	RouteWriter *kafka.Writer
	AlertWriter *kafka.Writer
}

// NewKafkaClients constructs Kafka reader/writer handles against the
// supplied broker list. The reader is configured as part of a
// consumer group to enable horizontal scaling of routing-engine
// replicas with automatic partition rebalancing.
func NewKafkaClients(brokers []string) *KafkaClients {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		GroupID:        consumerGroupID,
		Topic:          topicRidershipRaw,
		MinBytes:       1,
		MaxBytes:       10e6,
		MaxWait:        500 * time.Millisecond,
		CommitInterval: time.Second,
	})

	routeWriter := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topicRouteRecommend,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		BatchTimeout: 50 * time.Millisecond,
	}

	alertWriter := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topicAlerts,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		BatchTimeout: 50 * time.Millisecond,
	}

	return &KafkaClients{Reader: reader, RouteWriter: routeWriter, AlertWriter: alertWriter}
}

// Close releases all underlying Kafka connections.
func (k *KafkaClients) Close() {
	if err := k.Reader.Close(); err != nil {
		log.Printf("kafka: error closing reader: %v", err)
	}
	if err := k.RouteWriter.Close(); err != nil {
		log.Printf("kafka: error closing route writer: %v", err)
	}
	if err := k.AlertWriter.Close(); err != nil {
		log.Printf("kafka: error closing alert writer: %v", err)
	}
}

// ConsumeLoop reads messages from the ridership topic and dispatches
// them onto the supplied channel for concurrent processing by the
// worker pool. It blocks until the context is cancelled, at which
// point it drains and returns, allowing the caller to perform a
// graceful shutdown.
func (k *KafkaClients) ConsumeLoop(ctx context.Context, out chan<- RidershipEvent) {
	for {
		msg, err := k.Reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				close(out)
				return
			}
			log.Printf("kafka: fetch error: %v", err)
			continue
		}

		var event RidershipEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("kafka: malformed event, skipping offset %d: %v", msg.Offset, err)
			_ = k.Reader.CommitMessages(ctx, msg)
			continue
		}

		select {
		case out <- event:
		case <-ctx.Done():
			close(out)
			return
		}

		if err := k.Reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("kafka: commit error at offset %d: %v", msg.Offset, err)
		}
	}
}

// PublishRecommendation emits a computed route recommendation to the
// downstream recommendations topic.
func (k *KafkaClients) PublishRecommendation(ctx context.Context, rec RouteRecommendation) error {
	payload, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	return k.RouteWriter.WriteMessages(ctx, kafka.Message{
		Key:   []byte(rec.OriginStationID),
		Value: payload,
		Time:  time.Now(),
	})
}

// PublishAlert emits a congestion alert to the downstream alerts topic.
func (k *KafkaClients) PublishAlert(ctx context.Context, alert AlertEvent) error {
	payload, err := json.Marshal(alert)
	if err != nil {
		return err
	}
	return k.AlertWriter.WriteMessages(ctx, kafka.Message{
		Key:   []byte(alert.StationID),
		Value: payload,
		Time:  time.Now(),
	})
}
