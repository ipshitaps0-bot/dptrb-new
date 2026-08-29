package main

import "time"

// RidershipEvent is the canonical schema consumed from the
// "ridership.events.raw" Kafka topic. It represents a single
// normalized turnstile tap emitted by the ingestion plane.
type RidershipEvent struct {
	StationID      string    `json:"station_id"`
	TurnstileID    string    `json:"turnstile_id"`
	Direction      string    `json:"direction"` // "IN" or "OUT"
	PassengerCount int32     `json:"passenger_count"`
	Timestamp      time.Time `json:"timestamp"`
	SequenceID     string    `json:"sequence_id"`
}

// RouteRecommendation is emitted to the "routing.recommendations"
// topic whenever a congestion-triggered recomputation produces a
// materially different optimal path for an affected station pair.
type RouteRecommendation struct {
	OriginStationID      string    `json:"origin_station_id"`
	DestinationStationID string    `json:"destination_station_id"`
	Path                 []string  `json:"path"`
	EstimatedCost        float64   `json:"estimated_cost"`
	GeneratedAt          time.Time `json:"generated_at"`
}

// AlertSeverity enumerates the tiered severity classification
// applied to congestion alerts.
type AlertSeverity string

const (
	SeverityAdvisory AlertSeverity = "ADVISORY"
	SeverityWarning  AlertSeverity = "WARNING"
	SeverityCritical AlertSeverity = "CRITICAL"
)

// AlertEvent is emitted to the "routing.alerts" topic when an
// edge's occupancy ratio crosses a configured congestion threshold.
type AlertEvent struct {
	StationID   string        `json:"station_id"`
	EdgeTarget  string        `json:"edge_target"`
	Severity    AlertSeverity `json:"severity"`
	LoadRatio   float64       `json:"load_ratio"`
	CurrentLoad int32         `json:"current_load"`
	Capacity    int32         `json:"capacity"`
	Timestamp   time.Time     `json:"timestamp"`
}

// Station represents a static vertex in the transit topology graph.
type Station struct {
	ID       string
	Name     string
	Platform string
}
