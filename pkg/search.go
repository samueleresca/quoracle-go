package pkg

import (
	"fmt"
	"time"
)

type SearchOptions struct {
	Optimize      OptimizeType
	ReadFraction  Distribution
	WriteFraction Distribution
	Resilience    uint
	F             int
	TimeoutSecs   float64
	LoadLimit     *float64
	NetworkLimit  *float64
	LatencyLimit  *float64
}

type SearchResult struct {
	QuorumSystem QuorumSystem
	Strategy     Strategy
}

func initSearchOptions(initOptions SearchOptions) func(options *SearchOptions) error {
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

func partitionings(xs []GenericExpr) chan [][]GenericExpr {
	return partitioningsHelper(xs)
}

func partitioningsHelper(xs []GenericExpr) chan [][]GenericExpr {
	chnl := make(chan [][]GenericExpr)
	if len(xs) == 0 {
		go func() {
			chnl <- [][]GenericExpr{}
			close(chnl)
		}()
		return chnl
	}

	x := xs[0]
	rest := xs[1:]

	go func() {
		for partition := range partitioningsHelper(rest) {
			newPartition := partition
			newPartition = append([][]GenericExpr{{x}}, newPartition...)

			chnl <- newPartition

			for i := 0; i < len(partition); i++ {
				result := make([][]GenericExpr, 0)
				result = append(result, partition[:i]...)
				result = append(result, append([]GenericExpr{x}, partition[i]...))

				chnl <- append(result, partition[i+1:]...)

			}
		}
		close(chnl)
	}()
	return chnl
}

func dupFreeExprs(nodes []GenericExpr, maxHeight int) chan GenericExpr {
	chnl := make(chan GenericExpr, 0)

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
				choose, _ := DefChoose(k, nodes)
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

			for _, subexprs := range productInterfaces(subiterators...) {

				exprs := make([]GenericExpr, 0)

				for _, se := range subexprs {
					exprs = append(exprs, se.(GenericExpr))
				}

				for k := 1; k < len(subexprs)+1; k++ {
					result, _ := DefChoose(k, exprs)
					chnl <- result
				}
			}
		}

		close(chnl)
	}()

	return chnl
}

func performQuorumSearch(nodes []GenericExpr, opts ...func(options *SearchOptions) error) (SearchResult, error) {

	sb := &SearchOptions{}
	// ... (write initializations with default values)...
	for _, op := range opts {
		err := op(sb)
		if err != nil {
			return SearchResult{}, err
		}
	}

	start := time.Now()

	metric := func(sigma Strategy) (*float64, error) {
		if sb.Optimize == Load {
			return sigma.Load(&sb.ReadFraction, &sb.WriteFraction)
		} else if sb.Optimize == Network {
			return sigma.NetworkLoad(&sb.ReadFraction, &sb.WriteFraction)
		} else {
			return sigma.Latency(&sb.ReadFraction, &sb.WriteFraction)
		}
	}

	var optQS *QuorumSystem = nil
	var optSigma *Strategy = nil
	var optMetric *float64 = nil

	doSearch := func(exprs chan GenericExpr) error {

		for r := range exprs {
			qs := DefQuorumSystemWithReads(r)

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

			sigma, err := qs.Strategy(initStrategyOptions(stratOpts))

			if err != nil {
				fmt.Printf("Strategy not found %s \n", err)
				continue
			}

			sigmaMetric, err := metric(*sigma)

			if err != nil {
				fmt.Printf("Calc strategy err %s \n", err)
				continue
			}

			if optMetric == nil || *sigmaMetric < *optMetric {
				optQS = &qs
				optSigma = sigma
				optMetric = sigmaMetric
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
		return SearchResult{}, fmt.Errorf("Error in search")
	}

	return SearchResult{
		QuorumSystem: *optQS,
		Strategy:     *optSigma,
	}, nil

}

func Search(nodes []GenericExpr, option SearchOptions) (SearchResult, error) {
	return performQuorumSearch(nodes, initSearchOptions(option))
}
