package pkg

import (
	"fmt"
	"gotest.tools/assert"
	"math"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func TestInit(t *testing.T) {

	assertQuorums := func(e Quorum, xs [][]string) {
		actual := make([]string, 0)

		for q := range e.Quorums() {
			var tmp []string

			for n := range q {
				tmp = append(tmp, n.String())
			}
			sort.Strings(tmp)
			actual = append(actual, strings.Join(tmp, ""))
		}

		var expected []string

		for _, x := range xs {
			sort.Strings(x)
			expected = append(expected, strings.Join(x, ""))
		}

		sort.Strings(actual)
		sort.Strings(expected)

		assert.Assert(t, reflect.DeepEqual(actual, expected) == true, fmt.Sprintf("assertQuorums - Actual: %v | Expected  %v", actual, expected))
	}
	a, b, c := NewNode("a"), NewNode("b"), NewNode("c")

	qs := NewQuorumSystemWithReads(a.Add(b))
	assertQuorums(qs.reads, [][]string{{"a"}, {"b"}})
	assertQuorums(qs.writes, [][]string{{"a", "b"}})

	qs = NewQuorumSystemWithWrites(a.Add(b))
	assertQuorums(qs.writes, [][]string{{"a"}, {"b"}})
	assertQuorums(qs.reads, [][]string{{"a", "b"}})

	qs, _ = NewQuorumSystem(a.Add(b), a.Multiply(b).Multiply(c))
	assertQuorums(qs.reads, [][]string{{"a"}, {"b"}})
	assertQuorums(qs.writes, [][]string{{"a", "b", "c"}})

	_, err := NewQuorumSystem(a.Add(b), a)

	assert.Error(t, err, "not all read quorums intersect all write quorums")

}

func TestUniformStrategy(t *testing.T) {

	assertSigma := func(actual []SigmaRecord, expected []SigmaRecord) {
		actualString := make([]string, 0)

		for _, element := range actual {
			var tmp []string
			prob := element.Probability

			for expr := range element.Quorum {
				tmp = append(tmp, expr.String())
			}

			sort.Strings(tmp)

			final := strings.Join(tmp, "")
			s := fmt.Sprintf("%f", prob)
			final += s

			actualString = append(actualString, final)
		}

		expectedString := make([]string, 0)

		for _, element := range expected {
			var tmp []string
			prob := element.Probability

			for expr := range element.Quorum {
				tmp = append(tmp, expr.String())
			}

			sort.Strings(tmp)

			final := strings.Join(tmp, "")
			s := fmt.Sprintf("%f", prob)
			final += s

			expectedString = append(expectedString, final)
		}

		sort.Strings(actualString)
		sort.Strings(expectedString)

		assert.Assert(t, reflect.DeepEqual(actualString, expectedString), "", fmt.Sprintf("assertSigma - Actual: %v | Expected  %v", actualString, expectedString))
	}

	a, b, c, d := NewNode("a"), NewNode("b"), NewNode("c"), NewNode("d")

	sigma, _ := NewQuorumSystemWithReads(a).UniformStrategy(0)

	assertSigma(sigma.SigmaR.Values, []SigmaRecord{{ExprSet{a: true}, 1.0}})
	assertSigma(sigma.SigmaW.Values, []SigmaRecord{{ExprSet{a: true}, 1.0}})

	sigma, _ = NewQuorumSystemWithReads(a.Add(a)).UniformStrategy(0)

	assertSigma(sigma.SigmaR.Values, []SigmaRecord{{ExprSet{a: true}, 1.0}})
	assertSigma(sigma.SigmaW.Values, []SigmaRecord{{ExprSet{a: true}, 1.0}})

	sigma, _ = NewQuorumSystemWithReads(a.Multiply(a)).UniformStrategy(0)

	assertSigma(sigma.SigmaR.Values, []SigmaRecord{{ExprSet{a: true}, 1.0}})
	assertSigma(sigma.SigmaW.Values, []SigmaRecord{{ExprSet{a: true}, 1.0}})

	sigma, _ = NewQuorumSystemWithReads(a.Add(a.Multiply(b))).UniformStrategy(0)

	assertSigma(sigma.SigmaR.Values, []SigmaRecord{{ExprSet{a: true}, 1.0}})
	assertSigma(sigma.SigmaW.Values, []SigmaRecord{{ExprSet{a: true}, 1.0}})

	sigma, _ = NewQuorumSystemWithReads(a.Add(a.Multiply(b)).Add(a.Multiply(c))).UniformStrategy(0)

	assertSigma(sigma.SigmaR.Values, []SigmaRecord{{ExprSet{a: true}, 1.0}})
	assertSigma(sigma.SigmaW.Values, []SigmaRecord{{ExprSet{a: true}, 1.0}})

	sigma, _ = NewQuorumSystemWithReads(a.Add(b)).UniformStrategy(0)

	assertSigma(sigma.SigmaR.Values, []SigmaRecord{
		{ExprSet{a: true}, 0.5},
		{ExprSet{b: true}, 0.5},
	})
	assertSigma(sigma.SigmaW.Values, []SigmaRecord{
		{ExprSet{a: true, b: true}, 1.0},
	})

	sigma, _ = NewQuorumSystemWithReads(a.Add(b).Add(c)).UniformStrategy(0)

	assertSigma(sigma.SigmaR.Values, []SigmaRecord{
		{ExprSet{a: true}, 1.0 / 3},
		{ExprSet{b: true}, 1.0 / 3},
		{ExprSet{c: true}, 1.0 / 3},
	})
	assertSigma(sigma.SigmaW.Values, []SigmaRecord{
		{ExprSet{a: true, b: true, c: true}, 1.0},
	})

	sigma, _ = NewQuorumSystemWithReads((a.Multiply(b)).Add(c.Multiply(d))).UniformStrategy(0)

	assertSigma(sigma.SigmaR.Values, []SigmaRecord{
		{ExprSet{a: true, b: true}, 1.0 / 2},
		{ExprSet{c: true, d: true}, 1.0 / 2},
	})
	assertSigma(sigma.SigmaW.Values, []SigmaRecord{
		{ExprSet{a: true, c: true}, 1.0 / 4},
		{ExprSet{a: true, d: true}, 1.0 / 4},
		{ExprSet{b: true, c: true}, 1.0 / 4},
		{ExprSet{b: true, d: true}, 1.0 / 4},
	})

	sigma, _ = NewQuorumSystemWithReads((a.Multiply(b)).Add(c.Multiply(d)).Add(a.Multiply(b)).Add(a.Multiply(b).Multiply(c))).UniformStrategy(0)

	assertSigma(sigma.SigmaR.Values, []SigmaRecord{
		{ExprSet{a: true, b: true}, 1.0 / 2},
		{ExprSet{c: true, d: true}, 1.0 / 2},
	})
	assertSigma(sigma.SigmaW.Values, []SigmaRecord{
		{ExprSet{a: true, c: true}, 1.0 / 4},
		{ExprSet{a: true, d: true}, 1.0 / 4},
		{ExprSet{b: true, c: true}, 1.0 / 4},
		{ExprSet{b: true, d: true}, 1.0 / 4},
	})
}

func TestMakeStrategy(t *testing.T) {
	assertSigma := func(actual []SigmaRecord, expected []SigmaRecord) {
		actualString := make([]string, 0)

		for _, element := range actual {
			var tmp []string
			prob := element.Probability

			for expr := range element.Quorum {
				tmp = append(tmp, expr.String())
			}

			sort.Strings(tmp)

			final := strings.Join(tmp, "")
			s := fmt.Sprintf("%f", prob)
			final += s

			actualString = append(actualString, final)
		}

		expectedString := make([]string, 0)

		for _, element := range expected {
			var tmp []string
			prob := element.Probability

			for expr := range element.Quorum {
				tmp = append(tmp, expr.String())
			}

			sort.Strings(tmp)

			final := strings.Join(tmp, "")
			s := fmt.Sprintf("%f", prob)
			final += s

			expectedString = append(expectedString, final)
		}

		sort.Strings(actualString)
		sort.Strings(expectedString)

		assert.Assert(t, reflect.DeepEqual(actualString, expectedString), "", fmt.Sprintf("assertSigma - Actual: %v | Expected  %v", actualString, expectedString))
	}

	a, b, c, d := NewNode("a"), NewNode("b"), NewNode("c"), NewNode("d")

	sigma, _ :=
		NewQuorumSystemWithReads(a.Multiply(b).Add(c.Multiply(d))).MakeStrategy(
			Sigma{Values: []SigmaRecord{
				{ExprSet{a: true, b: true}, 25},
				{ExprSet{c: true, d: true}, 75}}},
			Sigma{Values: []SigmaRecord{
				{ExprSet{a: true, c: true}, 1},
				{ExprSet{a: true, d: true}, 1},
				{ExprSet{b: true, c: true}, 1},
				{ExprSet{b: true, d: true}, 1}}})

	assertSigma(sigma.SigmaR.Values, []SigmaRecord{
		{ExprSet{a: true, b: true}, 0.25},
		{ExprSet{c: true, d: true}, 0.75}})

	assertSigma(sigma.SigmaW.Values, []SigmaRecord{
		{ExprSet{a: true, c: true}, 0.25},
		{ExprSet{a: true, d: true}, 0.25},
		{ExprSet{b: true, c: true}, 0.25},
		{ExprSet{b: true, d: true}, 0.25}})

	_, err :=
		NewQuorumSystemWithReads(a.Multiply(b).Add(c.Multiply(d))).MakeStrategy(
			Sigma{Values: []SigmaRecord{
				{ExprSet{a: true, b: true}, -1},
				{ExprSet{c: true, d: true}, 1}}},
			Sigma{Values: []SigmaRecord{
				{ExprSet{a: true, c: true}, 1},
				{ExprSet{a: true, d: true}, 1},
				{ExprSet{b: true, c: true}, 1},
				{ExprSet{b: true, d: true}, 1}}})

	assert.Assert(t, err != nil)

	_, err =
		NewQuorumSystemWithReads(a.Multiply(b).Add(c.Multiply(d))).MakeStrategy(
			Sigma{Values: []SigmaRecord{
				{ExprSet{a: true}, 1},
				{ExprSet{c: true, d: true}, 1}}},
			Sigma{Values: []SigmaRecord{
				{map[Expr]bool{a: true, c: true}, 1},
				{map[Expr]bool{a: true, d: true}, 1},
				{map[Expr]bool{b: true, c: true}, 1},
				{map[Expr]bool{b: true, d: true}, 1}}})

	assert.Assert(t, err != nil)
}

func TestOptimalStrategyLoad(t *testing.T) {

	const float64EqualityThreshold = 1e-9

	a, b, c, d :=
		NewNodeWithCapacityAndLatency("a", 2, 1, 1), NewNodeWithCapacityAndLatency("b", 2, 1, 2),
		NewNodeWithCapacityAndLatency("c", 2, 1, 3), NewNodeWithCapacityAndLatency("d", 2, 1, 4)

	qs := NewQuorumSystemWithReads((a.Multiply(b)).Add(c.Multiply(d)))

	// Load optimized
	strategyOptions := StrategyOptions{
		Optimize: Load,
		ReadFraction: QuorumDistribution{
			values: DistributionValues{1: 1}},
	}

	load, _ := qs.Load(strategyOptions)
	assert.Assert(t, math.Abs(*load-0.25) <= float64EqualityThreshold)

	cap, _ := qs.Capacity(strategyOptions)
	assert.Assert(t, math.Abs(*cap-4) <= float64EqualityThreshold)

	strategyOptions = StrategyOptions{
		Optimize: Load,
		WriteFraction: QuorumDistribution{
			values: map[Fraction]Weight{1: 1}},
	}

	load, _ = qs.Load(strategyOptions)
	assert.Assert(t, math.Abs(*load-0.5) <= float64EqualityThreshold)

	cap, _ = qs.Capacity(strategyOptions)
	assert.Assert(t, math.Abs(*cap-2) <= float64EqualityThreshold)

	networkLimit := 2.0
	strategyOptions = StrategyOptions{
		Optimize:     Load,
		NetworkLimit: &networkLimit,
		ReadFraction: QuorumDistribution{
			values: map[Fraction]Weight{1: 1}},
	}

	load, _ = qs.Load(strategyOptions)
	assert.Assert(t, math.Abs(*load-0.25) <= float64EqualityThreshold)

	cap, _ = qs.Capacity(strategyOptions)
	assert.Assert(t, math.Abs(*cap-4) <= float64EqualityThreshold)

	strategyOptions = StrategyOptions{
		Optimize:     Load,
		NetworkLimit: &networkLimit,
		WriteFraction: QuorumDistribution{
			values: map[Fraction]Weight{1: 1}},
	}

	load, _ = qs.Load(strategyOptions)
	assert.Assert(t, math.Abs(*load-0.5) <= float64EqualityThreshold)

	cap, _ = qs.Capacity(strategyOptions)
	assert.Assert(t, math.Abs(*cap-2) <= float64EqualityThreshold)

	latencyLimit := 4.0
	strategyOptions = StrategyOptions{
		Optimize:     Load,
		LatencyLimit: &latencyLimit,
		ReadFraction: QuorumDistribution{
			values: map[Fraction]Weight{1: 1}},
	}

	load, _ = qs.Load(strategyOptions)
	assert.Assert(t, math.Abs(*load-0.25) <= float64EqualityThreshold)

	cap, _ = qs.Capacity(strategyOptions)
	assert.Assert(t, math.Abs(*cap-4) <= float64EqualityThreshold)

	strategyOptions = StrategyOptions{
		Optimize:     Load,
		LatencyLimit: &latencyLimit,
		WriteFraction: QuorumDistribution{
			values: map[Fraction]Weight{1: 1}},
	}

	load, _ = qs.Load(strategyOptions)
	assert.Assert(t, math.Abs(*load-0.5) <= float64EqualityThreshold)

	cap, _ = qs.Capacity(strategyOptions)
	assert.Assert(t, math.Abs(*cap-2) <= float64EqualityThreshold)

	strategyOptions = StrategyOptions{
		Optimize: Load,
		F:        1,
		ReadFraction: QuorumDistribution{
			values: map[Fraction]Weight{1: 1}},
	}

	load, _ = qs.Load(strategyOptions)
	assert.Assert(t, math.Abs(*load-0.5) <= float64EqualityThreshold)

	strategyOptions = StrategyOptions{
		Optimize: Load,
		F:        1,
		ReadFraction: QuorumDistribution{
			values: map[Fraction]Weight{1: 1}},
	}

	cap, _ = qs.Capacity(strategyOptions)
	assert.Assert(t, math.Abs(*cap-2) <= float64EqualityThreshold)

	strategyOptions = StrategyOptions{
		Optimize: Load,
		F:        1,
		WriteFraction: QuorumDistribution{
			values: map[Fraction]Weight{1: 1}},
	}

	load, _ = qs.Load(strategyOptions)
	assert.Assert(t, math.Abs(*load-1) <= float64EqualityThreshold)

	strategyOptions = StrategyOptions{
		Optimize: Load,
		F:        1,
		WriteFraction: QuorumDistribution{
			values: map[Fraction]Weight{1: 1}},
	}

	cap, _ = qs.Capacity(strategyOptions)
	assert.Assert(t, math.Abs(*cap-1) <= float64EqualityThreshold)
}

func TestOptimalStrategyNetwork(t *testing.T) {

	const float64EqualityThreshold = 1e-9

	a, b, c, d :=
		NewNodeWithCapacityAndLatency("a", 2, 1, 1), NewNodeWithCapacityAndLatency("b", 2, 1, 2),
		NewNodeWithCapacityAndLatency("c", 2, 1, 3), NewNodeWithCapacityAndLatency("d", 2, 1, 4)

	qs := NewQuorumSystemWithReads((a.Multiply(b)).Add(c.Multiply(d)))

	// Network optimized
	strategyOptions := StrategyOptions{
		Optimize: Network,
		ReadFraction: QuorumDistribution{
			values: map[Fraction]Weight{1: 1}},
	}
	networkLoad, _ := qs.NetworkLoad(strategyOptions)
	assert.Assert(t, math.Abs(*networkLoad-2) <= float64EqualityThreshold)

	strategyOptions = StrategyOptions{
		Optimize: Network,
		WriteFraction: QuorumDistribution{
			values: map[Fraction]Weight{1: 1}},
	}
	networkLoad, _ = qs.NetworkLoad(strategyOptions)
	assert.Assert(t, math.Abs(*networkLoad-2) <= float64EqualityThreshold)

	loadLimit := 0.25
	strategyOptions = StrategyOptions{
		Optimize:  Network,
		LoadLimit: &loadLimit,
		ReadFraction: QuorumDistribution{
			values: map[Fraction]Weight{1: 1}},
	}
	networkLoad, _ = qs.NetworkLoad(strategyOptions)
	assert.Assert(t, math.Abs(*networkLoad-2) <= float64EqualityThreshold)

	loadLimit = 0.5
	strategyOptions = StrategyOptions{
		Optimize:  Network,
		LoadLimit: &loadLimit,
		WriteFraction: QuorumDistribution{
			values: map[Fraction]Weight{1: 1}},
	}
	networkLoad, _ = qs.NetworkLoad(strategyOptions)
	assert.Assert(t, math.Abs(*networkLoad-2) <= float64EqualityThreshold)

	latencyLimit := 2.0

	strategyOptions = StrategyOptions{
		Optimize:     Network,
		LatencyLimit: &latencyLimit,
		ReadFraction: QuorumDistribution{
			values: map[Fraction]Weight{1: 1}},
	}
	networkLoad, _ = qs.NetworkLoad(strategyOptions)
	assert.Assert(t, math.Abs(*networkLoad-2) <= float64EqualityThreshold)

	latencyLimit = 3.0

	strategyOptions = StrategyOptions{
		Optimize:     Network,
		LatencyLimit: &latencyLimit,
		WriteFraction: QuorumDistribution{
			values: map[Fraction]Weight{1: 1}},
	}
	networkLoad, _ = qs.NetworkLoad(strategyOptions)
	assert.Assert(t, math.Abs(*networkLoad-2) <= float64EqualityThreshold)

	strategyOptions = StrategyOptions{
		Optimize: Network,
		F:        1,
		ReadFraction: QuorumDistribution{
			values: map[Fraction]Weight{1: 1}},
	}
	networkLoad, _ = qs.NetworkLoad(strategyOptions)
	assert.Assert(t, math.Abs(*networkLoad-4) <= float64EqualityThreshold)

	strategyOptions = StrategyOptions{
		Optimize: Network,
		F:        1,
		WriteFraction: QuorumDistribution{
			values: map[Fraction]Weight{1: 1}},
	}
	networkLoad, _ = qs.NetworkLoad(strategyOptions)
	assert.Assert(t, math.Abs(*networkLoad-4) <= float64EqualityThreshold)
}

func TestOptimalStrategyLatency(t *testing.T) {

	const float64EqualityThreshold = 1e-9

	a, b, c, d :=
		NewNodeWithCapacityAndLatency("a", 2, 1, 1), NewNodeWithCapacityAndLatency("b", 2, 1, 2),
		NewNodeWithCapacityAndLatency("c", 2, 1, 3), NewNodeWithCapacityAndLatency("d", 2, 1, 4)

	qs := NewQuorumSystemWithReads((a.Multiply(b)).Add(c.Multiply(d)))

	// Latency optimized
	strategyOptions := StrategyOptions{
		Optimize: Latency,
		ReadFraction: QuorumDistribution{
			values: map[Fraction]Weight{1: 1}},
	}

	latency, _ := qs.Latency(strategyOptions)
	assert.Assert(t, math.Abs(*latency-2) <= float64EqualityThreshold, fmt.Sprintf("Actual:%f", *latency))

	strategyOptions = StrategyOptions{
		Optimize: Latency,
		WriteFraction: QuorumDistribution{
			values: map[Fraction]Weight{1: 1}},
	}

	latency, _ = qs.Latency(strategyOptions)
	assert.Assert(t, math.Abs(*latency-3) <= float64EqualityThreshold, fmt.Sprintf("Actual:%f", *latency))

	loadLimit := 1.0

	strategyOptions = StrategyOptions{
		Optimize:  Latency,
		LoadLimit: &loadLimit,
		ReadFraction: QuorumDistribution{
			values: map[Fraction]Weight{1: 1}},
	}

	latency, _ = qs.Latency(strategyOptions)
	assert.Assert(t, math.Abs(*latency-2) <= float64EqualityThreshold, fmt.Sprintf("Actual:%f", *latency))

	loadLimit = 1.0

	strategyOptions = StrategyOptions{
		Optimize:  Latency,
		LoadLimit: &loadLimit,
		WriteFraction: QuorumDistribution{
			values: map[Fraction]Weight{1: 1}},
	}

	latency, _ = qs.Latency(strategyOptions)
	assert.Assert(t, math.Abs(*latency-3) <= float64EqualityThreshold, fmt.Sprintf("Actual:%f", *latency))

	networkLimit := 2.0

	strategyOptions = StrategyOptions{
		Optimize:     Latency,
		NetworkLimit: &networkLimit,
		ReadFraction: QuorumDistribution{
			values: map[Fraction]Weight{1: 1}},
	}

	latency, _ = qs.Latency(strategyOptions)
	assert.Assert(t, math.Abs(*latency-2) <= float64EqualityThreshold, fmt.Sprintf("Actual:%f", *latency))

	networkLimit = 2.0

	strategyOptions = StrategyOptions{
		Optimize:     Latency,
		NetworkLimit: &networkLimit,
		WriteFraction: QuorumDistribution{
			values: map[Fraction]Weight{1: 1}},
	}

	latency, _ = qs.Latency(strategyOptions)
	assert.Assert(t, math.Abs(*latency-3) <= float64EqualityThreshold, fmt.Sprintf("Actual:%f", *latency))

	strategyOptions = StrategyOptions{
		Optimize: Latency,
		F:        1,
		ReadFraction: QuorumDistribution{
			values: map[Fraction]Weight{1: 1}},
	}
	latency, _ = qs.Latency(strategyOptions)
	assert.Assert(t, math.Abs(*latency-2) <= float64EqualityThreshold, fmt.Sprintf("Actual:%f", *latency))

	strategyOptions = StrategyOptions{
		Optimize: Latency,
		F:        1,
		WriteFraction: QuorumDistribution{
			values: map[Fraction]Weight{1: 1}},
	}
	latency, _ = qs.Latency(strategyOptions)
	assert.Assert(t, math.Abs(*latency-3) <= float64EqualityThreshold, fmt.Sprintf("Actual:%f", *latency))
}

func TestOptimalStrategyIllegalSpecs(t *testing.T) {
	a, b, c, d :=
		NewNodeWithCapacityAndLatency("a", 2, 1, 1), NewNodeWithCapacityAndLatency("b", 2, 1, 2),
		NewNodeWithCapacityAndLatency("c", 2, 1, 3), NewNodeWithCapacityAndLatency("d", 2, 1, 4)

	qs := NewQuorumSystemWithReads((a.Multiply(b)).Add(c.Multiply(d)))

	loadLimit := 1.0
	strategyOptions := StrategyOptions{
		Optimize:  Load,
		LoadLimit: &loadLimit,
		ReadFraction: QuorumDistribution{
			values: map[Fraction]Weight{1: 1}},
	}

	_, err := qs.Load(strategyOptions)

	assert.Assert(t, err.Error() == "a getLoadObjective limit cannot be set when optimizing for getLoadObjective")

	networkLimit := 1.0
	strategyOptions = StrategyOptions{
		Optimize:     Network,
		NetworkLimit: &networkLimit,
		ReadFraction: QuorumDistribution{
			values: map[Fraction]Weight{1: 1}},
	}

	_, err = qs.Load(strategyOptions)

	assert.Assert(t, err.Error() == "a network limit cannot be set when optimizing for network")

	latencyLimit := 1.0
	strategyOptions = StrategyOptions{
		Optimize:     Latency,
		LatencyLimit: &latencyLimit,
		ReadFraction: QuorumDistribution{
			values: map[Fraction]Weight{1: 1}},
	}

	_, err = qs.Load(strategyOptions)

	assert.Assert(t, err.Error() == "a latency limit cannot be set when optimizing for latency")

}

func TestOptimalStrategyUnsatisfiableConstraints(t *testing.T) {
	a, b, c, d :=
		NewNodeWithCapacityAndLatency("a", 2, 1, 1), NewNodeWithCapacityAndLatency("b", 2, 1, 2),
		NewNodeWithCapacityAndLatency("c", 2, 1, 3), NewNodeWithCapacityAndLatency("d", 2, 1, 4)

	qs := NewQuorumSystemWithReads((a.Multiply(b)).Add(c.Multiply(d)))

	networkLimit := 1.5

	strategyOptions := StrategyOptions{
		Optimize:     Load,
		NetworkLimit: &networkLimit,
		WriteFraction: QuorumDistribution{
			values: map[Fraction]Weight{1: 1}},
	}

	_, err := qs.Load(strategyOptions)
	assert.Assert(t, err.Error() == "no optimal strategy found")

	latencyLimit := 2.0

	strategyOptions = StrategyOptions{
		Optimize:     Load,
		LatencyLimit: &latencyLimit,
		WriteFraction: QuorumDistribution{
			values: map[Fraction]Weight{1: 1}},
	}

	_, err = qs.Load(strategyOptions)
	assert.Assert(t, err.Error() == "no optimal strategy found")

	latencyLimit = 2.0
	loadLimit := 0.25

	strategyOptions = StrategyOptions{
		Optimize:     Network,
		LatencyLimit: &latencyLimit,
		LoadLimit:    &loadLimit,
		ReadFraction: QuorumDistribution{
			values: map[Fraction]Weight{1: 1}},
	}

	//_, err := qs.Load(strategyOptions)
	//assert.Assert(t, err.Error() == "no optimal strategy found")
}
