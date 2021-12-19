package pkg

import (
	"fmt"
	"testing"
)

func TestStrategyUseCase(t *testing.T) {
	a, b, c, d :=
		NewNodeWithCapacityAndLatency("a", 2, 1, 1),
		NewNodeWithCapacityAndLatency("b", 2, 1, 2),
		NewNodeWithCapacityAndLatency("c", 2, 1, 3),
		NewNodeWithCapacityAndLatency("d", 2, 1, 4)

	// Read quorum (a*b) + (c*d)
	qs := NewQuorumSystemWithReads((a.Multiply(b)).Add(c.Multiply(d)))

	// Load optimized strategy with read_fraction 100%
	strategyOptions := StrategyOptions{
		Optimize: Load,
		ReadFraction: QuorumDistribution{
			values: DistributionValues{1: 1}},
	}

	load, _ := qs.Load(strategyOptions)
	capacity, _ := qs.Capacity(strategyOptions)
	networkLoad, _ := qs.NetworkLoad(strategyOptions)
	latency, _ := qs.Latency(strategyOptions)

	fmt.Println(fmt.Sprintf("Load: %f | Capacity: %f | Network load: %f | Latency: %f",
		load, capacity, networkLoad, latency))
}

func TestSearchUseCase(t *testing.T) {
	a, b, c, d :=
		NewNodeWithCapacityAndLatency("a", 2, 1, 1),
		NewNodeWithCapacityAndLatency("b", 2, 1, 2),
		NewNodeWithCapacityAndLatency("c", 2, 1, 3),
		NewNodeWithCapacityAndLatency("d", 2, 1, 4)

	so := SearchOptions{
		Optimize:     Load,
		ReadFraction: QuorumDistribution{DistributionValues{0.5: 1.0}},
	}

	sr, err := Search(so, a, b, c, d)

	if err != nil {
		fmt.Println(err.Error())
	}

	fmt.Println(sr.Strategy.GetReadQuorum())
	fmt.Println(sr.Strategy.GetWriteQuorum())
}
