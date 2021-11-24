package pkg

import (
	"fmt"
	"testing"
)

func TestExample(t *testing.T) {
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
			values: map[Fraction]Weight{1: 1}},
	}

	load, _ := qs.Load(strategyOptions)

	fmt.Println(load)
}
