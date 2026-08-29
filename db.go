package main

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// batchFlushInterval controls how frequently accumulated ridership
// events are flushed to PostgreSQL as a single bulk insert, trading
// bounded durability latency for a substantial reduction in
// per-event transactional overhead.
const batchFlushInterval = 2 * time.Second

// batchMaxSize forces an early flush if the in-memory buffer grows
// past this threshold, bounding worst-case memory usage and
// recovery-window data loss during a crash.
const batchMaxSize = 5000

// BatchedEventWriter accumulates RidershipEvent records in memory and
// periodically persists them to PostgreSQL using pgx's native bulk
// COPY protocol, decoupling ingestion-rate cardinality from
// per-transaction database overhead.
type BatchedEventWriter struct {
	pool   *pgxpool.Pool
	mu     sync.Mutex
	buffer []RidershipEvent
}

// NewBatchedEventWriter constructs a writer bound to the given
// connection pool.
func NewBatchedEventWriter(pool *pgxpool.Pool) *BatchedEventWriter {
	return &BatchedEventWriter{pool: pool, buffer: make([]RidershipEvent, 0, batchMaxSize)}
}

// Enqueue adds an event to the pending write buffer. If the buffer
// has reached its size threshold, a synchronous flush is triggered
// immediately rather than waiting for the next ticker interval.
func (w *BatchedEventWriter) Enqueue(ctx context.Context, event RidershipEvent) {
	w.mu.Lock()
	w.buffer = append(w.buffer, event)
	shouldFlush := len(w.buffer) >= batchMaxSize
	w.mu.Unlock()

	if shouldFlush {
		w.flush(ctx)
	}
}

// Run starts the periodic flush loop and blocks until ctx is
// cancelled, at which point it performs one final flush before
// returning to guarantee no buffered events are dropped on shutdown.
func (w *BatchedEventWriter) Run(ctx context.Context) {
	ticker := time.NewTicker(batchFlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			w.flush(ctx)
		case <-ctx.Done():
			w.flush(context.Background())
			return
		}
	}
}

func (w *BatchedEventWriter) flush(ctx context.Context) {
	w.mu.Lock()
	if len(w.buffer) == 0 {
		w.mu.Unlock()
		return
	}
	pending := w.buffer
	w.buffer = make([]RidershipEvent, 0, batchMaxSize)
	w.mu.Unlock()

	rows := make([][]interface{}, 0, len(pending))
	for _, e := range pending {
		rows = append(rows, []interface{}{
			e.StationID, e.TurnstileID, e.Direction, e.PassengerCount, e.Timestamp, e.SequenceID,
		})
	}

	copyCount, err := w.pool.CopyFrom(
		ctx,
		[]string{"ridership_events"},
		[]string{"station_id", "turnstile_id", "direction", "passenger_count", "event_timestamp", "sequence_id"},
		&sliceCopySource{rows: rows},
	)
	if err != nil {
		log.Printf("db: batch flush failed for %d events: %v", len(pending), err)
		return
	}
	log.Printf("db: flushed %d ridership events to persistent store", copyCount)
}

// sliceCopySource adapts an in-memory row slice to pgx's CopyFromSource
// interface, avoiding an external dependency for a straightforward
// bulk-load use case.
type sliceCopySource struct {
	rows [][]interface{}
	pos  int
}

func (s *sliceCopySource) Next() bool {
	if s.pos >= len(s.rows) {
		return false
	}
	s.pos++
	return true
}

func (s *sliceCopySource) Values() ([]interface{}, error) {
	return s.rows[s.pos-1], nil
}

func (s *sliceCopySource) Err() error {
	return nil
}

// HydrateGraph loads the static topology (stations and edges) from
// PostgreSQL into an in-memory Graph on service startup, ensuring
// routing continuity across restarts without requiring a full replay
// of the Kafka event log.
func HydrateGraph(ctx context.Context, pool *pgxpool.Pool, g *Graph) error {
	stationRows, err := pool.Query(ctx, `SELECT station_id, name, platform_count FROM stations`)
	if err != nil {
		return err
	}
	for stationRows.Next() {
		var id, name string
		var platformCount int
		if err := stationRows.Scan(&id, &name, &platformCount); err != nil {
			stationRows.Close()
			return err
		}
		g.AddStation(&Station{ID: id, Name: name})
	}
	stationRows.Close()

	edgeRows, err := pool.Query(ctx, `SELECT origin_station_id, destination_station_id, base_weight, capacity FROM route_edges`)
	if err != nil {
		return err
	}
	defer edgeRows.Close()
	for edgeRows.Next() {
		var from, to string
		var baseWeight float64
		var capacity int32
		if err := edgeRows.Scan(&from, &to, &baseWeight, &capacity); err != nil {
			return err
		}
		if err := g.AddEdge(from, to, baseWeight, capacity); err != nil {
			log.Printf("db: skipping malformed edge %s->%s: %v", from, to, err)
		}
	}
	return edgeRows.Err()
}
