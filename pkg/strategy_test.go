package pkg

import (
	"fmt"
	"gotest.tools/assert"
	"math"
	"testing"
)

func TestGetQuorum(t *testing.T) {
	a, b, c, d, e := DefNode("a"), DefNode("b"), DefNode("c"), DefNode("d"), DefNode("e")

	choose1, _ := DefChoose(2, []GenericExpr{a, b, c})
	choose2, _ := DefChoose(2, []GenericExpr{a, b, c, d, e})

	exprs := []GenericExpr{a,
		a.Add(b),
		a.Add(b).Add(c),
		choose1,
		choose2,
		(a.Add(b)).Multiply(c.Add(d)),
		(a.Multiply(b)).Add(c.Multiply(d)),
	}

	for _, expr := range exprs {
		qs := DefQuorumSystemWithReads(expr)
		sigma, _ := qs.UniformStrategy(0)

		for i := 0; i < 10; i++ {
			assert.Assert(t, qs.IsReadQuorum(sigma.GetReadQuorum()))
			assert.Assert(t, qs.IsWriteQuorum(sigma.GetWriteQuorum()))
		}

	}
}

func TestNetworkLoad(t *testing.T) {
	a, b, c, d, e := DefNode("a"), DefNode("b"), DefNode("c"), DefNode("d"), DefNode("e")

	qs := DefQuorumSystemWithReads(a.Multiply(b).Add(c.Multiply(d).Multiply(e)))
	sigma, _ := qs.MakeStrategy(
		Sigma{Values: []SigmaRecord{
			{ExprSet{a: true, b: true}, 75},
			{ExprSet{c: true, d: true, e: true}, 25},
		}},
		Sigma{Values: []SigmaRecord{
			{ExprSet{a: true, c: true}, 5},
			{ExprSet{a: true, d: true}, 10},
			{ExprSet{a: true, e: true}, 15},
			{ExprSet{b: true, c: true}, 20},
			{ExprSet{b: true, d: true}, 25},
			{ExprSet{b: true, e: true}, 25},
		}})

	var rf, wf Distribution
	rf = QuorumDistribution{values: map[Fraction]Weight{0.8: 1}}
	wf = nil
	result, _ := sigma.NetworkLoad(&rf, &wf)

	assert.Equal(t, *result, 0.8*0.75*2+0.8*0.25*3+0.2*2, fmt.Sprintf("Result: %d", result))
}

func TestLatency(t *testing.T) {
	const float64EqualityThreshold = 1e-9

	a, b, c, d, e := DefNodeWithLatency("a", 1), DefNodeWithLatency("b", 2), DefNodeWithLatency("c", 3), DefNodeWithLatency("d", 4), DefNodeWithLatency("e", 5)

	qs := DefQuorumSystemWithReads(a.Multiply(b).Add(c.Multiply(d).Multiply(e)))
	sigma, _ := qs.MakeStrategy(
		Sigma{Values: []SigmaRecord{
			{ExprSet{a: true, b: true}, 10},
			{ExprSet{a: true, b: true, c: true}, 20},
			{ExprSet{c: true, d: true, e: true}, 30},
			{ExprSet{c: true, d: true, e: true, a: true}, 40},
		}},
		Sigma{Values: []SigmaRecord{
			{ExprSet{a: true, c: true}, 5},
			{ExprSet{a: true, d: true}, 10},
			{ExprSet{a: true, e: true}, 15},
			{ExprSet{b: true, c: true}, 20},
			{ExprSet{b: true, d: true}, 25},
			{ExprSet{b: true, e: true}, 25},
		}})

	var rf, wf Distribution
	rf = QuorumDistribution{values: map[Fraction]Weight{0.8: 1}}
	wf = nil
	result, _ := sigma.Latency(&rf, &wf)

	assert.Assert(t, math.Abs(*result-(0.8*0.10*2+
		0.8*0.20*2+
		0.8*0.30*5+
		0.8*0.40*5+
		0.2*0.05*3+
		0.2*0.10*4+
		0.2*0.15*5+
		0.2*0.20*3+
		0.2*0.25*4+
		0.2*0.25*5)) <= float64EqualityThreshold, fmt.Sprintf("Result: %d", result))
}
