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

func TestLoadCapUtil(t *testing.T) {
	a, b, c, d := DefNodeWithCapacity("a", 50, 10), DefNodeWithCapacity("b", 60, 20), DefNodeWithCapacity("c", 70, 30), DefNodeWithCapacity("d", 80, 40)
	const float64EqualityThreshold = 1e-9

	qs := DefQuorumSystemWithReads(a.Multiply(b).Add(c.Multiply(d)))

	sigma, _ := qs.MakeStrategy(
		Sigma{Values: []SigmaRecord{
			{ExprSet{a: true, b: true}, 0.75},
			{ExprSet{c: true, d: true}, 0.25},
		}},
		Sigma{Values: []SigmaRecord{
			{ExprSet{a: true, c: true}, 0.1},
			{ExprSet{a: true, d: true}, 0.2},
			{ExprSet{b: true, c: true}, 0.3},
			{ExprSet{b: true, d: true}, 0.4},
		}})

	nodeLoads08 := map[Node]float64{
		a: 0.8/50*0.75 + 0.2/10*(0.1+0.2),
		b: 0.8/60*0.75 + 0.2/20*(0.3+0.4),
		c: 0.8/70*0.25 + 0.2/30*(0.1+0.3),
		d: 0.8/80*0.25 + 0.2/40*(0.2+0.4),
	}

	load08 := 0.0

	for _, v := range nodeLoads08 {
		if v >= load08 {
			load08 = v
		}
	}

	cap08 := 1 / load08

	nodeThroughputs08 := map[Node]float64{
		a: cap08 * (0.8*0.75 + 0.2*(0.1+0.2)),
		b: cap08 * (0.8*0.75 + 0.2*(0.3+0.4)),
		c: cap08 * (0.8*0.25 + 0.2*(0.1+0.3)),
		d: cap08 * (0.8*0.25 + 0.2*(0.2+0.4)),
	}

	var rf, wf Distribution
	rf = QuorumDistribution{values: map[Fraction]Weight{0.8: 1}}
	wf = nil

	load, _ := sigma.Load(&rf, &wf)
	capacity, _ := sigma.Capacity(&rf, &wf)

	assert.Assert(t, math.Abs(*load-load08) <= float64EqualityThreshold)
	assert.Assert(t, math.Abs(*capacity-cap08) <= float64EqualityThreshold)

	for n, l := range nodeLoads08 {
		utilization, _ := sigma.NodeUtilization(n, &rf, &wf)
		loadN, _ := sigma.NodeLoad(n, &rf, &wf)

		assert.Assert(t, math.Abs(*loadN-l) <= float64EqualityThreshold)
		assert.Assert(t, math.Abs(*utilization-(l*cap08)) <= float64EqualityThreshold)
	}

	for n, th := range nodeThroughputs08 {
		throughput, _ := sigma.NodeThroughput(n, &rf, &wf)
		assert.Assert(t, math.Abs(*throughput-th) <= float64EqualityThreshold)
	}

	nodeLoads05 := map[Node]float64{
		a: 0.5/50*0.75 + 0.5/10*(0.1+0.2),
		b: 0.5/60*0.75 + 0.5/20*(0.3+0.4),
		c: 0.5/70*0.25 + 0.5/30*(0.1+0.3),
		d: 0.5/80*0.25 + 0.5/40*(0.2+0.4),
	}

	load05 := 0.0

	for _, v := range nodeLoads05 {
		if v >= load05 {
			load05 = v
		}
	}

	// 0.5
	cap05 := 1 / load05

	nodeThroughputs05 := map[Node]float64{
		a: cap05 * (0.5*0.75 + 0.5*(0.1+0.2)),
		b: cap05 * (0.5*0.75 + 0.5*(0.3+0.4)),
		c: cap05 * (0.5*0.25 + 0.5*(0.1+0.3)),
		d: cap05 * (0.5*0.25 + 0.5*(0.2+0.4)),
	}

	rf = QuorumDistribution{values: map[Fraction]Weight{0.5: 1}}
	wf = nil

	load, _ = sigma.Load(&rf, &wf)
	capacity, _ = sigma.Capacity(&rf, &wf)

	assert.Assert(t, math.Abs(*load-load05) <= float64EqualityThreshold)
	assert.Assert(t, math.Abs(*capacity-cap05) <= float64EqualityThreshold)

	for n, l := range nodeLoads05 {
		utilization, _ := sigma.NodeUtilization(n, &rf, &wf)
		loadN, _ := sigma.NodeLoad(n, &rf, &wf)

		assert.Assert(t, math.Abs(*loadN-l) <= float64EqualityThreshold)
		assert.Assert(t, math.Abs(*utilization-(l*cap05)) <= float64EqualityThreshold)
	}

	for n, th := range nodeThroughputs05 {
		throughput, _ := sigma.NodeThroughput(n, &rf, &wf)
		assert.Assert(t, math.Abs(*throughput-th) <= float64EqualityThreshold)
	}

	// dynamic read fraction values

	rf = QuorumDistribution{values: map[float64]float64{0.8: 0.7, 0.5: 0.3}}

	nodeLoads := map[Node]float64{
		a: 0.7*(0.8/50*0.75+0.2/10*(0.1+0.2)) +
			0.3*(0.5/50*0.75+0.5/10*(0.1+0.2)),
		b: 0.7*(0.8/60*0.75+0.2/20*(0.3+0.4)) +
			0.3*(0.5/60*0.75+0.5/20*(0.3+0.4)),
		c: 0.7*(0.8/70*0.25+0.2/30*(0.1+0.3)) +
			0.3*(0.5/70*0.25+0.5/30*(0.1+0.3)),
		d: 0.7*(0.8/80*0.25+0.2/40*(0.2+0.4)) +
			0.3*(0.5/80*0.25+0.5/40*(0.2+0.4)),
	}

	loadTotal := (0.7 * load08) + (0.3 * load05)
	capTotal := (0.7 * cap08) + (0.3 * cap05)

	load, _ = sigma.Load(&rf, &wf)
	capacity, _ = sigma.Capacity(&rf, &wf)

	assert.Assert(t, math.Abs(*load-loadTotal) <= float64EqualityThreshold)
	assert.Assert(t, math.Abs(*capacity-capTotal) <= float64EqualityThreshold)

	nodeThroughputs := map[Node]float64{
		a: cap08*0.7*(0.8*0.75+0.2*(0.1+0.2)) +
			cap05*0.3*(0.5*0.75+0.5*(0.1+0.2)),
		b: cap08*0.7*(0.8*0.75+0.2*(0.3+0.4)) +
			cap05*0.3*(0.5*0.75+0.5*(0.3+0.4)),
		c: cap08*0.7*(0.8*0.25+0.2*(0.1+0.3)) +
			cap05*0.3*(0.5*0.25+0.5*(0.1+0.3)),
		d: cap08*0.7*(0.8*0.25+0.2*(0.2+0.4)) +
			cap05*0.3*(0.5*0.25+0.5*(0.2+0.4)),
	}

	for n, l := range nodeLoads {

		nodeLoad, _ := sigma.NodeLoad(n, &rf, &wf)
		nodeUtil, _ := sigma.NodeUtilization(n, &rf, &wf)

		assert.Assert(t, math.Abs(*nodeLoad-l) <= float64EqualityThreshold)
		assert.Assert(t, math.Abs(*nodeUtil-(0.3*nodeLoads05[n]*cap05+0.7*cap08*nodeLoads08[n])) <= float64EqualityThreshold)
	}

	for n, th := range nodeThroughputs {
		nodeL, _ := sigma.NodeThroughput(n, &rf, &wf)
		assert.Assert(t, math.Abs(*nodeL-th) <= float64EqualityThreshold)
	}

}
