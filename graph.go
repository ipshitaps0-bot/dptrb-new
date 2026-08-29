package main

import (
	"fmt"
	"math"
	"sync"
)

// congestionExponentBase controls the steepness of the exponential
// penalty applied to edges approaching capacity saturation. A value
// greater than 1.0 causes near-capacity edges to be aggressively
// deprioritized in shortest-path computation, biasing recommended
// routes away from overcrowded segments well before hard saturation.
const congestionExponentBase = 6.0

// congestionAlertThreshold is the load-ratio (CurrentLoad/Capacity)
// above which an edge mutation triggers alert emission and
// downstream route recomputation for affected station pairs.
const congestionAlertThreshold = 0.75

// Edge represents a directed transit segment between two stations.
type Edge struct {
	To         string
	BaseWeight float64
	Capacity   int32

	mu          sync.RWMutex
	currentLoad int32
}

// EffectiveWeight returns the congestion-adjusted traversal cost of
// the edge. As occupancy approaches capacity, the cost grows
// exponentially rather than linearly, so the shortest-path search
// naturally routes around near-saturated segments.
func (e *Edge) EffectiveWeight() float64 {
	e.mu.RLock()
	load := e.currentLoad
	cap := e.Capacity
	e.mu.RUnlock()

	if cap <= 0 {
		return e.BaseWeight
	}
	ratio := math.Min(float64(load)/float64(cap), 1.0)
	penalty := math.Pow(congestionExponentBase, ratio) - 1.0
	return e.BaseWeight * (1.0 + penalty)
}

// ApplyLoadDelta atomically mutates the edge's occupancy counter and
// returns the resulting load ratio, allowing the caller to decide
// whether an alert or recomputation should be triggered.
func (e *Edge) ApplyLoadDelta(delta int32) (ratio float64) {
	e.mu.Lock()
	e.currentLoad += delta
	if e.currentLoad < 0 {
		e.currentLoad = 0
	}
	load := e.currentLoad
	cap := e.Capacity
	e.mu.Unlock()

	if cap <= 0 {
		return 0
	}
	return math.Min(float64(load)/float64(cap), 1.0)
}

func (e *Edge) snapshot() (load, capacity int32) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.currentLoad, e.Capacity
}

// Graph is a concurrency-safe directed weighted graph representing
// the static transit topology with dynamically mutable edge state.
// Structural mutation (AddStation/AddEdge) is protected independently
// from high-frequency load mutation (which is handled per-edge via
// Edge's own mutex), minimizing lock contention under concurrent
// worker access.
type Graph struct {
	mu        sync.RWMutex
	stations  map[string]*Station
	adjacency map[string][]*Edge
}

// NewGraph constructs an empty transit topology graph.
func NewGraph() *Graph {
	return &Graph{
		stations:  make(map[string]*Station),
		adjacency: make(map[string][]*Edge),
	}
}

// AddStation registers a vertex in the topology graph.
func (g *Graph) AddStation(s *Station) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.stations[s.ID] = s
	if _, ok := g.adjacency[s.ID]; !ok {
		g.adjacency[s.ID] = nil
	}
}

// AddEdge registers a directed segment between two previously
// registered stations.
func (g *Graph) AddEdge(from, to string, baseWeight float64, capacity int32) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, ok := g.stations[from]; !ok {
		return fmt.Errorf("graph: unknown origin station %q", from)
	}
	if _, ok := g.stations[to]; !ok {
		return fmt.Errorf("graph: unknown destination station %q", to)
	}
	g.adjacency[from] = append(g.adjacency[from], &Edge{
		To:         to,
		BaseWeight: baseWeight,
		Capacity:   capacity,
	})
	return nil
}

// Neighbors returns a snapshot slice of outbound edges for a station.
// The underlying Edge pointers remain live and concurrency-safe.
func (g *Graph) Neighbors(stationID string) []*Edge {
	g.mu.RLock()
	defer g.mu.RUnlock()
	edges := g.adjacency[stationID]
	out := make([]*Edge, len(edges))
	copy(out, edges)
	return out
}

// FindEdge locates the directed edge between two stations, if present.
func (g *Graph) FindEdge(from, to string) *Edge {
	g.mu.RLock()
	defer g.mu.RUnlock()
	for _, e := range g.adjacency[from] {
		if e.To == to {
			return e
		}
	}
	return nil
}

// StationIDs returns a snapshot of every registered vertex identifier.
func (g *Graph) StationIDs() []string {
	g.mu.RLock()
	defer g.mu.RUnlock()
	ids := make([]string, 0, len(g.stations))
	for id := range g.stations {
		ids = append(ids, id)
	}
	return ids
}
