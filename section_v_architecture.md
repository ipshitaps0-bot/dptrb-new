# Section V: System Architecture and Implementation Methodology

## V.1 Architectural Paradigm

The Dynamic Public Transport Ridership Balancer (DPTRB) is engineered as an **event-driven, microservice-oriented distributed system**, adhering to the principles of loose coupling, horizontal scalability, and eventual consistency. The architecture rejects a monolithic request-response model in favor of an **asynchronous publish-subscribe backbone**, which allows ingestion throughput and routing computation to scale independently under variable passenger load. The system is partitioned into four logical planes:

1. **Ingestion Plane** — high-frequency turnstile event capture and normalization.
2. **Streaming Plane** — durable, partitioned message transport via Apache Kafka.
3. **Computation Plane** — a Go-based concurrent routing optimization engine.
4. **Persistence & Alerting Plane** — PostgreSQL for durable state, plus a real-time notification subsystem.

This separation of concerns ensures that a burst in turnstile telemetry (e.g., during peak-hour commuter surges) does not degrade the latency of route-recommendation computation, since the two planes communicate exclusively through Kafka's durable log rather than direct synchronous calls.

## V.2 Data Ingestion Pipeline

Turnstile hardware at each station emits discrete tap events (entry/exit, station identifier, turnstile identifier, timestamp) at sub-second granularity. A lightweight ingestion microservice, implemented in Python for rapid I/O-bound throughput and ecosystem compatibility with edge-device SDKs, performs the following responsibilities:

- **Schema normalization**: raw turnstile payloads (which may vary by hardware vendor) are coerced into a canonical `RidershipEvent` schema.
- **Idempotency enforcement**: a deterministic composite key (`turnstile_id + timestamp + sequence`) is attached to prevent duplicate counting during network retries.
- **Partition-aware production**: events are published to the Kafka topic `ridership.events.raw`, partitioned by `station_id`, guaranteeing per-station ordering while enabling parallel consumption across brokers.

Backpressure is handled via Kafka's own log-based buffering; the ingestion layer never blocks on downstream computation, decoupling hardware-facing latency from analytical latency.

## V.3 Concurrent Routing Optimization Engine

The computational core of DPTRB is a Go service exploiting the language's native concurrency primitives (goroutines, channels, and the `sync` package) to achieve high-throughput, low-latency graph recomputation. The transit network is modeled as a **weighted, directed graph** where:

- **Vertices** represent stations.
- **Edges** represent direct transit segments, each carrying a `BaseWeight` (nominal travel time/cost) and a mutable `CurrentLoad` counter reflecting live occupancy.

A **congestion-adjusted edge weight function** is applied at query time, exponentially penalizing edges approaching capacity saturation, which produces route recommendations that proactively divert projected ridership away from near-capacity segments rather than reactively responding after overcrowding occurs.

The engine follows a **fan-out worker pool pattern**: a single Kafka consumer goroutine reads from `ridership.events.raw` and dispatches events onto a buffered channel; a configurable pool of worker goroutines (default: `runtime.NumCPU() * 2`) concurrently apply load-deltas to the shared graph structure under fine-grained `sync.RWMutex` protection, ensuring read-heavy path-computation queries are not serialized behind write operations. When a station-pair's effective edge weight crosses a configured congestion threshold, the affected worker triggers an **on-demand shortest-path recomputation** (a concurrency-safe Dijkstra implementation backed by a binary heap priority queue) and emits both a route-recommendation and, where applicable, an overcrowding alert to dedicated Kafka output topics.

## V.4 Real-Time Alerting Subsystem

Alerts are modeled as a distinct event class (`AlertEvent`) rather than overloading the routing-recommendation schema, preserving single-responsibility semantics per topic. Alert severity is tiered (`ADVISORY`, `WARNING`, `CRITICAL`) based on the ratio of `CurrentLoad` to station/edge `Capacity`, enabling downstream consumers (operations dashboards, commuter-facing applications) to apply differentiated UX treatment without additional computation.

## V.5 Persistence Layer

PostgreSQL serves as the system of record for entities requiring ACID guarantees and relational integrity: station metadata, the static topology graph, and historical ridership aggregates used for offline model retraining and capacity-planning analytics. Write amplification from high-frequency turnstile events is mitigated via a **batched, ticker-flushed asynchronous writer goroutine**, which accumulates events in-memory and performs bulk `COPY`-style inserts at a fixed interval, rather than issuing a discrete transaction per event. This decouples ingestion cardinality from database write-transaction overhead by an order of magnitude.

## V.6 Scalability and Fault Tolerance

- **Horizontal scalability** is achieved at the Kafka partition level: increasing partition count on `ridership.events.raw` linearly increases achievable consumer parallelism.
- **Fault isolation**: consumer group rebalancing ensures that a routing-engine instance failure does not result in event loss, as uncommitted offsets are reprocessed by a surviving replica.
- **Graceful degradation**: the routing engine maintains an in-memory graph snapshot; on cold start, it hydrates state from PostgreSQL before resuming Kafka consumption, ensuring no discontinuity in route-quality guarantees after a restart.
- **Idempotent recomputation**: because edge weights are derived functions of cumulative load rather than incrementally-corrupted state, replayed events during rebalancing converge to identical graph state, preserving correctness under at-least-once delivery semantics.
