# quoracle-go

[![build](https://github.com/samueleresca/quoracle-go/actions/workflows/build.yml/badge.svg)](https://github.com/samueleresca/quoracle-go/actions/workflows/build.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/samueleresca/quoracle-go.svg)](https://pkg.go.dev/github.com/samueleresca/quoracle-go)

A Golang port of [mwhittaker/quoracle](https://github.com/mwhittaker/quoracle).
For more information check the original paper [Read-Write Quorum Systems Made Practical - Michael Whittaker, Aleksey Charapko, Joseph M. Hellerstein, Heidi Howard, Ion Stoica](https://mwhittaker.github.io/publications/quoracle.pdf).

## Requirements

This projects depends on [lanl/clp](https://github.com/lanl/clp) to solve the linear optimization problems.
[lanl/clp](https://github.com/lanl/clp) relies on `clp`, which needs to be installed on your machine [using the following instructions](https://github.com/coin-or/Clp#binaries).

## Get optimal strategy metrics

You can use `quoracle-go` to return the load, the capacity, the network load, and the latency of a strategy:

```go
package main

import "fmt"

func main() {
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
		load, capacity, networkLoad, latency)))
}
```

## Optimized strategy search

The library provides a way to search for the optimal strategy. Below the example:

```golang
package main

import "fmt"

func main() {
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
```

## References

- [Read-Write Quorum Systems Made Practical - Michael Whittaker, Aleksey Charapko, Joseph M. Hellerstein, Heidi Howard, Ion Stoica](https://mwhittaker.github.io/publications/quoracle.pdf)
- [mwhittaker/quoracle](https://github.com/mwhittaker/quoracle)
- [github.com/lanl/clp](https://github.com/lanl/clp)
- [coin-or/clp](https://github.com/coin-or/Clp)
