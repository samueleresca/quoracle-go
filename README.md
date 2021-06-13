# quoracle-go

A Golang port of [mwhittaker/quoracle](https://github.com/mwhittaker/quoracle).
For more information check the original paper [Read-Write Quorum Systems Made Practical - Michael Whittaker, Aleksey Charapko, Joseph M. Hellerstein, Heidi Howard, Ion Stoica](https://mwhittaker.github.io/publications/quoracle.pdf).

## Requirements

This projects depends on [lanl/clp](https://github.com/lanl/clp) to solve the linear optimization projects.
The [lanl/clp](https://github.com/lanl/clp) relies on `clp`, which needs to be installed on your machine using the following instructions:

[CLP download binaries](https://github.com/coin-or/Clp#binaries)

## Getting started

You can use `quoracle-go` as follow:

```go
a, b, c, d :=
		DefNodeWithCapacityAndLatency("a", 2, 1, 1), 
		DefNodeWithCapacityAndLatency("b", 2, 1, 2),
		DefNodeWithCapacityAndLatency("c", 2, 1, 3), 
		DefNodeWithCapacityAndLatency("d", 2, 1, 4)
    
// Read quorum (a*b) + (c*d) 
qs := DefQuorumSystemWithReads((a.Multiply(b)).Add(c.Multiply(d)))

// Load optimized strategy with read_fraction 100%
strategyOptions := StrategyOptions{
	Optimize: Load,
	ReadFraction: QuorumDistribution{
		values: map[Fraction]Weight{1: 1}},
}

load, _ := qs.Load(strategyOptions)
```


## Credits

- [Read-Write Quorum Systems Made Practical - Michael Whittaker, Aleksey Charapko, Joseph M. Hellerstein, Heidi Howard, Ion Stoica](https://mwhittaker.github.io/publications/quoracle.pdf)
- [mwhittaker/quoracle](https://github.com/mwhittaker/quoracle)
- [github.com/lanl/clp](https://github.com/lanl/clp)
- [coin-or/clp](https://github.com/coin-or/Clp)