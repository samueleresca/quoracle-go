package pkg

import (
	"fmt"
	"gotest.tools/assert"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func TestPartitions(t *testing.T) {

	node1, node2, node3, node4 := DefNode("1"), DefNode("2"), DefNode("3"), DefNode("4")

	for r := range partitionings([]GenericExpr{}) {
		assert.Assert(t, reflect.DeepEqual(r, [][]GenericExpr{}))
	}

	for r := range partitionings([]GenericExpr{node1}) {
		assert.Assert(t, reflect.DeepEqual(r, [][]GenericExpr{{node1}}))
	}

	result := partitionings([]GenericExpr{node1, node2})

	result1 := <-result
	result2 := <-result

	assert.Assert(t, reflect.DeepEqual(result1, [][]GenericExpr{{node1}, {node2}}) == true)
	assert.Assert(t, reflect.DeepEqual(result2, [][]GenericExpr{{node1, node2}}) == true)

	expected := map[string]bool{
		"[[1] [2] [3]]": true,
		"[[1 2] [3]]":   true,
		"[[2] [1 3]]":   true,
		"[[1] [2 3]]":   true,
		"[[1 2 3]]":     true,
	}

	index := 0
	for actual := range partitionings([]GenericExpr{node1, node2, node3}) {
		_, ok := expected[fmt.Sprint(actual)]
		assert.Assert(t, ok == true, actual)
		index++
	}

	expected = map[string]bool{
		"[[1] [2] [3] [4]]": true,
		"[[1 2] [3] [4]]":   true,
		"[[2] [1 3] [4]]":   true,
		"[[2] [3] [1 4]]":   true,
		"[[1] [2 3] [4]]":   true,
		"[[1] [3] [2 4]]":   true,
		"[[1] [2] [3 4]]":   true,
		"[[1 2] [3 4]]":     true,
		"[[1 3] [2 4]]":     true,
		"[[2 3] [1 4]]":     true,
		"[[1] [2 3 4]]":     true,
		"[[2] [1 3 4]]":     true,
		"[[3] [1 2 4]]":     true,
		"[[1 2 3] [4]]":     true,
		"[[1 2 3 4]]":       true,
	}

	index = 0
	for actual := range partitionings([]GenericExpr{node1, node2, node3, node4}) {
		_, ok := expected[fmt.Sprint(actual)]
		assert.Assert(t, ok == true, actual)
		index++
	}
}

func TestDupFreePartitions(t *testing.T) {
	a, b, c, d := DefNode("a"), DefNode("b"), DefNode("c"), DefNode("d")

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

	expected := [][][]string{
		{{"a"}},
	}

	index := 0

	for e := range dupFreeExprs([]GenericExpr{a}, 0) {
		assertQuorums(e, expected[index])
		index++
	}

	expected = [][][]string{
		{{"a"}, {"b"}}, {{"a", "b"}},
	}

	index = 0

	for e := range dupFreeExprs([]GenericExpr{a, b}, 0) {
		assertQuorums(e, expected[index])
		index++
	}

	expected = [][][]string{
		{{"a"}, {"b"}, {"c"}},
		{{"a", "b"}, {"b", "c"}, {"c", "a"}},
		{{"a", "b", "c"}},
	}

	index = 0

	for e := range dupFreeExprs([]GenericExpr{a, b, c}, 1) {
		assertQuorums(e, expected[index])
		index++
	}

	expected = [][][]string{
		{{"a"}, {"b"}, {"c"}, {"d"}},
		{{"a", "b"}, {"b", "c"}, {"c", "a"}, {"a", "d"}, {"b", "d"}, {"c", "d"}},
		{{"a", "b", "c"}, {"a", "b", "d"}, {"b", "c", "d"}, {"a", "c", "d"}},
		{{"a", "b", "c", "d"}},
	}

	index = 0

	for e := range dupFreeExprs([]GenericExpr{a, b, c, d}, 1) {
		assertQuorums(e, expected[index])
		index++
	}
}

func TestSearch(t *testing.T) {
	a, b, c, e, d, f := DefNodeWithCapacityAndLatency("a", 1, 1, 2),
		DefNodeWithCapacityAndLatency("b", 1, 1, 1),
		DefNodeWithCapacityAndLatency("c", 1, 1, 2),
		DefNodeWithCapacityAndLatency("d", 2, 2, 1),
		DefNodeWithCapacityAndLatency("e", 1, 1, 2),
		DefNodeWithCapacityAndLatency("f", 2, 2, 1)

	for _, fr := range []float64{0, 0.5, 1} {
		result, err := Search([]GenericExpr{a, b, c}, SearchOptions{Optimize: Load, ReadFraction: QuorumDistribution{map[Fraction]Weight{fr: 1.0}}})
		assert.Assert(t, err == nil)
		assert.Assert(t, len(result.Strategy.SigmaR.Values) > 0)
		assert.Assert(t, len(result.Strategy.SigmaW.Values) > 0)

		result, err = Search([]GenericExpr{a, b, c}, SearchOptions{Optimize: Network, ReadFraction: QuorumDistribution{map[Fraction]Weight{fr: 1.0}}})
		assert.Assert(t, err == nil)
		assert.Assert(t, len(result.Strategy.SigmaR.Values) > 0)
		assert.Assert(t, len(result.Strategy.SigmaW.Values) > 0)

		result, err = Search([]GenericExpr{a, b, c}, SearchOptions{Optimize: Latency, ReadFraction: QuorumDistribution{map[Fraction]Weight{fr: 1.0}}})
		assert.Assert(t, err == nil, err)
		assert.Assert(t, len(result.Strategy.SigmaR.Values) > 0)
		assert.Assert(t, len(result.Strategy.SigmaW.Values) > 0)

		result, err = Search([]GenericExpr{a, b, c}, SearchOptions{Optimize: Load, ReadFraction: QuorumDistribution{map[Fraction]Weight{fr: 1.0}}, F: 0, Resilience: 1.0})
		assert.Assert(t, err == nil, err)
		assert.Assert(t, len(result.Strategy.SigmaR.Values) > 0)
		assert.Assert(t, len(result.Strategy.SigmaW.Values) > 0)

		result, err = Search([]GenericExpr{a, b, c}, SearchOptions{Optimize: Load, ReadFraction: QuorumDistribution{map[Fraction]Weight{fr: 1.0}}, F: 1.0, Resilience: 0.0})
		assert.Assert(t, err == nil, err)
		assert.Assert(t, len(result.Strategy.SigmaR.Values) > 0)
		assert.Assert(t, len(result.Strategy.SigmaW.Values) > 0)
	}

	networkLimit := 3.0
	latencyLimit := 2.0

	result, err := Search([]GenericExpr{a, b, c}, SearchOptions{Optimize: Load, ReadFraction: QuorumDistribution{map[Fraction]Weight{0.25: 1.0}}, NetworkLimit: &networkLimit, LatencyLimit: &latencyLimit})
	assert.Assert(t, err == nil)
	assert.Assert(t, len(result.Strategy.SigmaR.Values) > 0)
	assert.Assert(t, len(result.Strategy.SigmaW.Values) > 0)

	timeoutSecs := 0.25
	for _, fr := range []float64{0, 0.5} {
		result, err := Search([]GenericExpr{a, b, c, d, e, f}, SearchOptions{Optimize: Load, ReadFraction: QuorumDistribution{map[Fraction]Weight{fr: 1.0}}, TimeoutSecs: timeoutSecs})
		assert.Assert(t, err == nil)
		assert.Assert(t, len(result.Strategy.SigmaR.Values) > 0)
		assert.Assert(t, len(result.Strategy.SigmaW.Values) > 0)

		result, err = Search([]GenericExpr{a, b, c, d, e, f}, SearchOptions{Optimize: Network, ReadFraction: QuorumDistribution{map[Fraction]Weight{fr: 1.0}}, TimeoutSecs: timeoutSecs})
		assert.Assert(t, err == nil)
		assert.Assert(t, len(result.Strategy.SigmaR.Values) > 0)
		assert.Assert(t, len(result.Strategy.SigmaW.Values) > 0)

		result, err = Search([]GenericExpr{a, b, c, d, e, f}, SearchOptions{Optimize: Latency, ReadFraction: QuorumDistribution{map[Fraction]Weight{fr: 1.0}}, TimeoutSecs: timeoutSecs})
		assert.Assert(t, err == nil, err)
		assert.Assert(t, len(result.Strategy.SigmaR.Values) > 0)
		assert.Assert(t, len(result.Strategy.SigmaW.Values) > 0)

		result, err = Search([]GenericExpr{a, b, c, d, e, f}, SearchOptions{Optimize: Load, ReadFraction: QuorumDistribution{map[Fraction]Weight{fr: 1.0}}, F: 0, Resilience: 1.0, TimeoutSecs: timeoutSecs})
		assert.Assert(t, err == nil, err)
		assert.Assert(t, len(result.Strategy.SigmaR.Values) > 0)
		assert.Assert(t, len(result.Strategy.SigmaW.Values) > 0)

		result, err = Search([]GenericExpr{a, b, c, d, e, f}, SearchOptions{Optimize: Load, ReadFraction: QuorumDistribution{map[Fraction]Weight{fr: 1.0}}, F: 1.0, Resilience: 0.0, TimeoutSecs: timeoutSecs})
		assert.Assert(t, err == nil, err)
		assert.Assert(t, len(result.Strategy.SigmaR.Values) > 0)
		assert.Assert(t, len(result.Strategy.SigmaW.Values) > 0)
	}

}
