package main

import (
	"context"
	"log"
	"time"
)

// WorkerPool consumes RidershipEvents from a shared channel and
// applies them to the graph concurrently. Each worker independently
// decides whether a congestion threshold breach warrants alert
// emission and downstream shortest-path recomputation, avoiding a
// centralized bottleneck.
type WorkerPool struct {
	graph        *Graph
	kafka        *KafkaClients
	dbWriter     *BatchedEventWriter
	poolSize     int
	stationPairs []StationPair
}

// StationPair defines an origin/destination pair that the engine
// actively monitors for route-recommendation recomputation whenever
// congestion is detected along the corridor connecting them.
type StationPair struct {
	Origin      string
	Destination string
}

// NewWorkerPool constructs a worker pool bound to the given graph,
// Kafka clients, persistence writer, and the set of monitored
// station pairs.
func NewWorkerPool(g *Graph, kc *KafkaClients, dbw *BatchedEventWriter, poolSize int, pairs []StationPair) *WorkerPool {
	return &WorkerPool{
		graph:        g,
		kafka:        kc,
		dbWriter:     dbw,
		poolSize:     poolSize,
		stationPairs: pairs,
	}
}

// Run launches poolSize worker goroutines consuming from the given
// channel and blocks until the channel is closed and all workers
// have drained their remaining work.
func (wp *WorkerPool) Run(ctx context.Context, events <-chan RidershipEvent) {
	done := make(chan struct{}, wp.poolSize)
	for i := 0; i < wp.poolSize; i++ {
		go func(workerID int) {
			wp.workerLoop(ctx, workerID, events)
			done <- struct{}{}
		}(i)
	}
	for i := 0; i < wp.poolSize; i++ {
		<-done
	}
}

func (wp *WorkerPool) workerLoop(ctx context.Context, workerID int, events <-chan RidershipEvent) {
	for event := range events {
		wp.dbWriter.Enqueue(ctx, event)
		wp.processEvent(ctx, event)
	}
	log.Printf("worker[%d]: channel closed, shutting down", workerID)
}

func (wp *WorkerPool) processEvent(ctx context.Context, event RidershipEvent) {
	// A turnstile "IN" event increments load on every outbound edge
	// from the station, modeling the passenger entering the platform
	// pool prior to boarding; an "OUT" event decrements it.
	delta := event.PassengerCount
	if event.Direction == "OUT" {
		delta = -delta
	}

	for _, edge := range wp.graph.Neighbors(event.StationID) {
		ratio := edge.ApplyLoadDelta(delta)
		if ratio < congestionAlertThreshold {
			continue
		}

		severity := SeverityAdvisory
		switch {
		case ratio >= 0.95:
			severity = SeverityCritical
		case ratio >= 0.85:
			severity = SeverityWarning
		}

		load, capacity := edge.snapshot()
		alert := AlertEvent{
			StationID:   event.StationID,
			EdgeTarget:  edge.To,
			Severity:    severity,
			LoadRatio:   ratio,
			CurrentLoad: load,
			Capacity:    capacity,
			Timestamp:   time.Now().UTC(),
		}
		if err := wp.kafka.PublishAlert(ctx, alert); err != nil {
			log.Printf("worker: failed to publish alert for %s->%s: %v", event.StationID, edge.To, err)
		}

		wp.recomputeAffectedRoutes(ctx, event.StationID)
	}
}

// recomputeAffectedRoutes triggers shortest-path recomputation for
// every monitored station pair whose corridor passes through the
// station that just experienced a congestion-threshold breach, and
// publishes any resulting recommendation.
func (wp *WorkerPool) recomputeAffectedRoutes(ctx context.Context, congestedStation string) {
	for _, pair := range wp.stationPairs {
		if pair.Origin != congestedStation && pair.Destination != congestedStation {
			continue
		}
		result := ShortestPath(wp.graph, pair.Origin, pair.Destination)
		if !result.Reachable {
			continue
		}
		rec := RouteRecommendation{
			OriginStationID:      pair.Origin,
			DestinationStationID: pair.Destination,
			Path:                 result.Path,
			EstimatedCost:        result.TotalCost,
			GeneratedAt:          time.Now().UTC(),
		}
		if err := wp.kafka.PublishRecommendation(ctx, rec); err != nil {
			log.Printf("worker: failed to publish recommendation for %s->%s: %v", pair.Origin, pair.Destination, err)
		}
	}
}
