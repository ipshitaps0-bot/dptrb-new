package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	brokers := splitAndTrim(mustEnv("KAFKA_BROKERS"))
	databaseURL := mustEnv("DATABASE_URL")
	poolSize := envIntOrDefault("WORKER_POOL_SIZE", runtime.NumCPU()*2)

	dbPool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		log.Fatalf("main: unable to establish database pool: %v", err)
	}
	defer dbPool.Close()

	graph := NewGraph()
	hydrationCtx, cancelHydration := context.WithTimeout(ctx, 30*time.Second)
	if err := HydrateGraph(hydrationCtx, dbPool, graph); err != nil {
		cancelHydration()
		log.Fatalf("main: graph hydration failed: %v", err)
	}
	cancelHydration()
	log.Printf("main: graph hydrated with %d stations", len(graph.StationIDs()))

	kafkaClients := NewKafkaClients(brokers)
	defer kafkaClients.Close()

	dbWriter := NewBatchedEventWriter(dbPool)
	go dbWriter.Run(ctx)

	monitoredPairs, err := loadMonitoredPairs(ctx, dbPool)
	if err != nil {
		log.Fatalf("main: unable to load monitored station pairs: %v", err)
	}
	log.Printf("main: monitoring %d station pairs for congestion-triggered recomputation", len(monitoredPairs))

	pool := NewWorkerPool(graph, kafkaClients, dbWriter, poolSize, monitoredPairs)

	events := make(chan RidershipEvent, poolSize*100)
	go kafkaClients.ConsumeLoop(ctx, events)

	log.Printf("main: routing engine online, worker pool size=%d", poolSize)
	pool.Run(ctx, events)
	log.Println("main: shutdown complete")
}

func loadMonitoredPairs(ctx context.Context, pool *pgxpool.Pool) ([]StationPair, error) {
	rows, err := pool.Query(ctx, `SELECT origin_station_id, destination_station_id FROM monitored_corridors`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var pairs []StationPair
	for rows.Next() {
		var p StationPair
		if err := rows.Scan(&p.Origin, &p.Destination); err != nil {
			return nil, err
		}
		pairs = append(pairs, p)
	}
	return pairs, rows.Err()
}

func mustEnv(key string) string {
	val, ok := os.LookupEnv(key)
	if !ok || val == "" {
		log.Fatalf("main: required environment variable %q is not set", key)
	}
	return val
}

func envIntOrDefault(key string, fallback int) int {
	val, ok := os.LookupEnv(key)
	if !ok || val == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(val)
	if err != nil {
		log.Printf("main: invalid integer for %q, falling back to %d", key, fallback)
		return fallback
	}
	return parsed
}

func splitAndTrim(csv string) []string {
	parts := strings.Split(csv, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		trimmed := strings.TrimSpace(p)
		if trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}
