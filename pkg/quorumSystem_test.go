package pkg

import (
	"fmt"
	"gotest.tools/assert"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func TestInit(t *testing.T) {

	assertQuorums := func(e GenericExpr, xs [][]string) {
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
	a, b, c := DefNode("a"), DefNode("b"), DefNode("c")

	qs := DefQuorumSystemWithReads(a.Add(b))
	assertQuorums(qs.Reads, [][]string{{"a"}, {"b"}})
	assertQuorums(qs.Writes, [][]string{{"a", "b"}})

	qs = DefQuorumSystemWithWrites(a.Add(b))
	assertQuorums(qs.Writes, [][]string{{"a"}, {"b"}})
	assertQuorums(qs.Reads, [][]string{{"a", "b"}})

	qs, _ = DefQuorumSystem(a.Add(b), a.Multiply(b).Multiply(c))
	assertQuorums(qs.Reads, [][]string{{"a"}, {"b"}})
	assertQuorums(qs.Writes, [][]string{{"a", "b", "c"}})

	_, err := DefQuorumSystem(a.Add(b), a)

	assert.Error(t, err, "Not all read quorums intersect all write quorums")

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

	a, b, c, d := DefNode("a"), DefNode("b"), DefNode("c"), DefNode("d")

	sigma, _ := DefQuorumSystemWithReads(a).UniformStrategy(0)

	assertSigma(sigma.SigmaR.Values, []SigmaRecord{{map[GenericExpr]bool{a: true}, 1.0}})
	assertSigma(sigma.SigmaW.Values, []SigmaRecord{{map[GenericExpr]bool{a: true}, 1.0}})

	sigma, _ = DefQuorumSystemWithReads(a.Add(a)).UniformStrategy(0)

	assertSigma(sigma.SigmaR.Values, []SigmaRecord{{map[GenericExpr]bool{a: true}, 1.0}})
	assertSigma(sigma.SigmaW.Values, []SigmaRecord{{map[GenericExpr]bool{a: true}, 1.0}})

	sigma, _ = DefQuorumSystemWithReads(a.Multiply(a)).UniformStrategy(0)

	assertSigma(sigma.SigmaR.Values, []SigmaRecord{{map[GenericExpr]bool{a: true}, 1.0}})
	assertSigma(sigma.SigmaW.Values, []SigmaRecord{{map[GenericExpr]bool{a: true}, 1.0}})

	sigma, _ = DefQuorumSystemWithReads(a.Add(a.Multiply(b))).UniformStrategy(0)

	assertSigma(sigma.SigmaR.Values, []SigmaRecord{{map[GenericExpr]bool{a: true}, 1.0}})
	assertSigma(sigma.SigmaW.Values, []SigmaRecord{{map[GenericExpr]bool{a: true}, 1.0}})

	sigma, _ = DefQuorumSystemWithReads(a.Add(a.Multiply(b)).Add(a.Multiply(c))).UniformStrategy(0)

	assertSigma(sigma.SigmaR.Values, []SigmaRecord{{map[GenericExpr]bool{a: true}, 1.0}})
	assertSigma(sigma.SigmaW.Values, []SigmaRecord{{map[GenericExpr]bool{a: true}, 1.0}})

	sigma, _ = DefQuorumSystemWithReads(a.Add(b)).UniformStrategy(0)

	assertSigma(sigma.SigmaR.Values, []SigmaRecord{
		{map[GenericExpr]bool{a: true}, 0.5},
		{map[GenericExpr]bool{b: true}, 0.5},
	})
	assertSigma(sigma.SigmaW.Values, []SigmaRecord{
		{map[GenericExpr]bool{a: true, b: true}, 1.0},
	})

	sigma, _ = DefQuorumSystemWithReads(a.Add(b).Add(c)).UniformStrategy(0)

	assertSigma(sigma.SigmaR.Values, []SigmaRecord{
		{map[GenericExpr]bool{a: true}, 1.0 / 3},
		{map[GenericExpr]bool{b: true}, 1.0 / 3},
		{map[GenericExpr]bool{c: true}, 1.0 / 3},
	})
	assertSigma(sigma.SigmaW.Values, []SigmaRecord{
		{map[GenericExpr]bool{a: true, b: true, c: true}, 1.0},
	})

	sigma, _ = DefQuorumSystemWithReads((a.Multiply(b)).Add(c.Multiply(d))).UniformStrategy(0)

	assertSigma(sigma.SigmaR.Values, []SigmaRecord{
		{map[GenericExpr]bool{a: true, b: true}, 1.0 / 2},
		{map[GenericExpr]bool{c: true, d: true}, 1.0 / 2},
	})
	assertSigma(sigma.SigmaW.Values, []SigmaRecord{
		{map[GenericExpr]bool{a: true, c: true}, 1.0 / 4},
		{map[GenericExpr]bool{a: true, d: true}, 1.0 / 4},
		{map[GenericExpr]bool{b: true, c: true}, 1.0 / 4},
		{map[GenericExpr]bool{b: true, d: true}, 1.0 / 4},
	})

	sigma, _ = DefQuorumSystemWithReads((a.Multiply(b)).Add(c.Multiply(d)).Add(a.Multiply(b)).Add(a.Multiply(b).Multiply(c))).UniformStrategy(0)

	assertSigma(sigma.SigmaR.Values, []SigmaRecord{
		{map[GenericExpr]bool{a: true, b: true}, 1.0 / 2},
		{map[GenericExpr]bool{c: true, d: true}, 1.0 / 2},
	})
	assertSigma(sigma.SigmaW.Values, []SigmaRecord{
		{map[GenericExpr]bool{a: true, c: true}, 1.0 / 4},
		{map[GenericExpr]bool{a: true, d: true}, 1.0 / 4},
		{map[GenericExpr]bool{b: true, c: true}, 1.0 / 4},
		{map[GenericExpr]bool{b: true, d: true}, 1.0 / 4},
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

	a, b, c, d := DefNode("a"), DefNode("b"), DefNode("c"), DefNode("d")

	sigma, _ :=
		DefQuorumSystemWithReads(a.Multiply(b).Add(c.Multiply(d))).MakeStrategy(
			Sigma{Values: []SigmaRecord{
				{map[GenericExpr]bool{a: true, b: true}, 25},
				{map[GenericExpr]bool{c: true, d: true}, 75}}},
			Sigma{Values: []SigmaRecord{
				{map[GenericExpr]bool{a: true, c: true}, 1},
				{map[GenericExpr]bool{a: true, d: true}, 1},
				{map[GenericExpr]bool{b: true, c: true}, 1},
				{map[GenericExpr]bool{b: true, d: true}, 1}}})

	assertSigma(sigma.SigmaR.Values, []SigmaRecord{
		{map[GenericExpr]bool{a: true, b: true}, 0.25},
		{map[GenericExpr]bool{c: true, d: true}, 0.75}})

	assertSigma(sigma.SigmaW.Values, []SigmaRecord{
		{map[GenericExpr]bool{a: true, c: true}, 0.25},
		{map[GenericExpr]bool{a: true, d: true}, 0.25},
		{map[GenericExpr]bool{b: true, c: true}, 0.25},
		{map[GenericExpr]bool{b: true, d: true}, 0.25}})

	_, err :=
		DefQuorumSystemWithReads(a.Multiply(b).Add(c.Multiply(d))).MakeStrategy(
			Sigma{Values: []SigmaRecord{
				{map[GenericExpr]bool{a: true, b: true}, -1},
				{map[GenericExpr]bool{c: true, d: true}, 1}}},
			Sigma{Values: []SigmaRecord{
				{map[GenericExpr]bool{a: true, c: true}, 1},
				{map[GenericExpr]bool{a: true, d: true}, 1},
				{map[GenericExpr]bool{b: true, c: true}, 1},
				{map[GenericExpr]bool{b: true, d: true}, 1}}})

	assert.Assert(t, err != nil)

	_, err =
		DefQuorumSystemWithReads(a.Multiply(b).Add(c.Multiply(d))).MakeStrategy(
			Sigma{Values: []SigmaRecord{
				{map[GenericExpr]bool{a: true}, 1},
				{map[GenericExpr]bool{c: true, d: true}, 1}}},
			Sigma{Values: []SigmaRecord{
				{map[GenericExpr]bool{a: true, c: true}, 1},
				{map[GenericExpr]bool{a: true, d: true}, 1},
				{map[GenericExpr]bool{b: true, c: true}, 1},
				{map[GenericExpr]bool{b: true, d: true}, 1}}})

	assert.Assert(t, err != nil)
}

func TestOptimalStrategy(t *testing.T) {

	a, b, c, d :=
		DefNodeWithCapacity("a", 2, 1, 1), DefNodeWithCapacity("b", 2, 1, 2),
		DefNodeWithCapacity("c", 2, 1, 3), DefNodeWithCapacity("d", 2, 1, 4)

	qs := DefQuorumSystemWithReads((a.Multiply(b)).Add(c.Multiply(d)))

	strategyOptions := StrategyOptions{
		Optimize: Latency,
		ReadFraction: QuorumDistribution{
			values: map[Fraction]Weight{1: 1}},
	}

	latency, _ := qs.Latency(strategyOptions)
	assert.Assert(t, *latency == 2)

	networkLoad, _ := qs.NetworkLoad(strategyOptions)
	assert.Assert(t, *networkLoad == 2)
}
