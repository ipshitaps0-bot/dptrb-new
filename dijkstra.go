package main

import (
	"container/heap"
	"math"
)

// pqItem is a single entry in the shortest-path priority queue.
type pqItem struct {
	stationID string
	cost      float64
	index     int
}

// priorityQueue implements heap.Interface over pqItem, ordered by
// ascending cumulative cost, forming the core of the Dijkstra
// traversal's frontier selection.
type priorityQueue []*pqItem

func (pq priorityQueue) Len() int           { return len(pq) }
func (pq priorityQueue) Less(i, j int) bool { return pq[i].cost < pq[j].cost }
func (pq priorityQueue) Swap(i, j int)      { pq[i], pq[j] = pq[j], pq[i]; pq[i].index = i; pq[j].index = j }
func (pq *priorityQueue) Push(x interface{}) {
	item := x.(*pqItem)
	item.index = len(*pq)
	*pq = append(*pq, item)
}
func (pq *priorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*pq = old[:n-1]
	return item
}

// ShortestPathResult captures the outcome of a single-source
// shortest-path computation.
type ShortestPathResult struct {
	Path      []string
	TotalCost float64
	Reachable bool
}

// ShortestPath computes the congestion-adjusted minimum-cost path
// between origin and destination using a binary-heap-backed Dijkstra
// traversal. Edge weights are read via Edge.EffectiveWeight(), so the
// result reflects live occupancy at the moment of invocation. The
// graph's own per-edge locking makes this safe to invoke concurrently
// from multiple worker goroutines without external synchronization.
func ShortestPath(g *Graph, origin, destination string) ShortestPathResult {
	dist := make(map[string]float64)
	prev := make(map[string]string)
	visited := make(map[string]bool)

	for _, id := range g.StationIDs() {
		dist[id] = math.Inf(1)
	}
	dist[origin] = 0

	pq := &priorityQueue{}
	heap.Init(pq)
	heap.Push(pq, &pqItem{stationID: origin, cost: 0})

	for pq.Len() > 0 {
		current := heap.Pop(pq).(*pqItem)
		if visited[current.stationID] {
			continue
		}
		visited[current.stationID] = true

		if current.stationID == destination {
			break
		}

		for _, edge := range g.Neighbors(current.stationID) {
			if visited[edge.To] {
				continue
			}
			candidate := dist[current.stationID] + edge.EffectiveWeight()
			if candidate < dist[edge.To] {
				dist[edge.To] = candidate
				prev[edge.To] = current.stationID
				heap.Push(pq, &pqItem{stationID: edge.To, cost: candidate})
			}
		}
	}

	if math.IsInf(dist[destination], 1) {
		return ShortestPathResult{Reachable: false}
	}

	path := []string{destination}
	for at := destination; at != origin; {
		p, ok := prev[at]
		if !ok {
			return ShortestPathResult{Reachable: false}
		}
		path = append([]string{p}, path...)
		at = p
	}

	return ShortestPathResult{
		Path:      path,
		TotalCost: dist[destination],
		Reachable: true,
	}
}
