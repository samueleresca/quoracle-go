package pkg

import (
	"fmt"
	"time"
)

// SearchOptions describes the options you can configure in the optimal strategy search.
type SearchOptions struct {
	//Optimize is the target of the optimization.
	Optimize OptimizeType
	//ReadFraction represents the read workload distribution.
	ReadFraction Distribution
	//WriteFraction represents the write workload distribution.
	WriteFraction Distribution
	//Resilience configures a resilience level threshold for the quorum system.
	Resilience uint
	// F r ∈ R is F-resilient for some integer f if despite removing
	// any f nodes from r, r is still a read quorum
	F uint
	//TimeoutSecs the number of seconds we can keep searching.
	TimeoutSecs float64
	//LoadLimit represents the load limit constraint.
	LoadLimit *float64
	//NetworkLimit represents the network limit constraint.
	NetworkLimit *float64
	//LatencyLimit represents the latency limit constraint.
	LatencyLimit *float64
}

// initializeSearchOptions returns an initialize function for SearchOptions.
func initializeSearchOptions(initOptions SearchOptions) func(options *SearchOptions) error {
	init := func(options *SearchOptions) error {
		options.Optimize = initOptions.Optimize
		options.LatencyLimit = initOptions.LatencyLimit
		options.NetworkLimit = initOptions.NetworkLimit
		options.LoadLimit = initOptions.LoadLimit
		options.F = initOptions.F
		options.ReadFraction = initOptions.ReadFraction
		options.WriteFraction = initOptions.WriteFraction
		options.TimeoutSecs = initOptions.TimeoutSecs
		options.Resilience = initOptions.Resilience

		return nil
	}
	return init
}

// SearchResult represent the result of the search of our optimal strategy.
type SearchResult struct {
	QuorumSystem QuorumSystem
	Strategy     Strategy
}

// Search given some nodes, and a SearchOptions instance, returns the optimal strategy and quorum system in respect of the optimization target and constraints.
func Search(nodes []Expr, option SearchOptions) (SearchResult, error) {
	return performQuorumSearch(nodes, initializeSearchOptions(option))
}

func performQuorumSearch(nodes []Expr, opts ...func(options *SearchOptions) error) (SearchResult, error) {

	sb := &SearchOptions{}

	// ... (write initializations with default values)...
	for _, op := range opts {
		err := op(sb)
		if err != nil {
			return SearchResult{}, err
		}
	}

	start := time.Now()

	var optQS *QuorumSystem = nil
	var optSigma *Strategy = nil
	var optMetric *float64 = nil

	getMetric := func(sigma Strategy) (float64, error) {
		if sb.Optimize == Load {
			return sigma.Load(&sb.ReadFraction, &sb.WriteFraction)
		}

		if sb.Optimize == Network {
			return sigma.NetworkLoad(&sb.ReadFraction, &sb.WriteFraction)
		}

		return sigma.Latency(&sb.ReadFraction, &sb.WriteFraction)
	}

	doSearch := func(exprs chan Expr) error {

		for r := range exprs {
			qs := NewQuorumSystemWithReads(r)

			if qs.Resilience() < sb.Resilience {
				continue
			}

			stratOpts := StrategyOptions{
				Optimize:      sb.Optimize,
				LoadLimit:     sb.LoadLimit,
				NetworkLimit:  sb.NetworkLimit,
				LatencyLimit:  sb.LatencyLimit,
				ReadFraction:  sb.ReadFraction,
				WriteFraction: sb.WriteFraction,
				F:             sb.F,
			}

			strategy, err := qs.Strategy(initializeStrategyOptions(stratOpts))

			if err != nil {
				fmt.Printf("Strategy not found %s \n", err)
				continue
			}

			sigmaMetric, err := getMetric(*strategy)

			if err != nil {
				fmt.Printf("Calc strategy err %s \n", err)
				continue
			}

			if optMetric == nil || sigmaMetric < *optMetric {
				optQS = &qs
				optSigma = strategy
				optMetric = &sigmaMetric
			}

			t := time.Now()
			elapsed := t.Sub(start)

			if sb.TimeoutSecs != 0 && elapsed.Seconds() > sb.TimeoutSecs {
				fmt.Printf("Timeout hit %f \n", sb.TimeoutSecs)
				return nil
			}
		}

		return nil
	}

	err := doSearch(dupFreeExprs(nodes, 2))

	if err != nil {
		return SearchResult{}, err
	}

	err = doSearch(dupFreeExprs(nodes, 0))

	if err != nil {
		return SearchResult{}, err
	}

	if optQS == nil {
		return SearchResult{}, fmt.Errorf("error in search")
	}

	return SearchResult{
		QuorumSystem: *optQS,
		Strategy:     *optSigma,
	}, nil
}

// dupFreeExprs returns all possible expressions over `nodes` with height at most max_height.
//The same expression can be returned multiple times.
func dupFreeExprs(nodes []Expr, maxHeight int) chan Expr {
	chnl := make(chan Expr, 0)

	if len(nodes) == 1 {

		go func() {
			chnl <- nodes[0]
			close(chnl)
		}()

		return chnl
	}

	if maxHeight == 1 {

		go func() {
			for k := 1; k < len(nodes)+1; k++ {
				choose, _ := NewChoose(k, nodes)
				chnl <- choose
			}
			close(chnl)
		}()

		return chnl
	}

	go func() {
		for partitioning := range partitionings(nodes) {
			if len(partitioning) == 1 {
				continue
			}

			subiterators := make([][]interface{}, 0)

			for _, p := range partitioning {
				tmp := make([]interface{}, 0)
				for e := range dupFreeExprs(p, maxHeight-1) {
					tmp = append(tmp, e)
				}

				subiterators = append(subiterators, tmp)
			}

			for _, subexprs := range product(subiterators...) {

				exprs := make([]Expr, 0)

				for _, se := range subexprs {
					exprs = append(exprs, se.(Expr))
				}

				for k := 1; k < len(subexprs)+1; k++ {
					result, _ := NewChoose(k, exprs)
					chnl <- result
				}
			}
		}

		close(chnl)
	}()

	return chnl
}

func partitionings(xs []Expr) chan [][]Expr {
	chnl := make(chan [][]Expr)
	if len(xs) == 0 {
		go func() {
			chnl <- [][]Expr{}
			close(chnl)
		}()
		return chnl
	}

	x := xs[0]
	rest := xs[1:]

	go func() {
		for partition := range partitionings(rest) {
			newPartition := partition
			newPartition = append([][]Expr{{x}}, newPartition...)

			chnl <- newPartition

			for i := 0; i < len(partition); i++ {
				result := make([][]Expr, 0)
				result = append(result, partition[:i]...)
				result = append(result, append([]Expr{x}, partition[i]...))

				chnl <- append(result, partition[i+1:]...)

			}
		}
		close(chnl)
	}()
	return chnl
}
