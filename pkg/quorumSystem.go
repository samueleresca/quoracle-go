package pkg

import (
	"fmt"
	"sort"
	"time"
)

type OptimizeType string

const (
	Load    OptimizeType = "Load"
	Network OptimizeType = "Network"
	Latency OptimizeType = "Latency"
)

type QuorumSystem struct {
	Reads   GenericExpr
	Writes  GenericExpr
	XtoNode map[string]Node
}

type StrategyOptions struct {
	Optimize      OptimizeType
	LoadLimit     *float64
	NetworkLimit  *float64
	LatencyLimit  *float64
	ReadFraction  Distribution
	WriteFraction Distribution
	F             int
}

func DefQuorumSystem(reads GenericExpr, writes GenericExpr) QuorumSystem {
	return QuorumSystem{}
}

func DefQuorumSystemWithReads(reads GenericExpr) QuorumSystem {
	return QuorumSystem{}
}

func DefQuorumSystemWithWrites(writes GenericExpr) QuorumSystem {
	return QuorumSystem{}
}

func (qs QuorumSystem) String() string {
	return ""
}

func (qs QuorumSystem) ReadQuorums() chan map[GenericExpr]bool {
	return qs.Reads.Quorums()
}

func (qs QuorumSystem) WriteQuorums() chan map[GenericExpr]bool {
	return qs.Writes.Quorums()
}

func (qs QuorumSystem) IsReadQuorum(xs map[GenericExpr]bool) bool {
	return qs.Reads.IsQuorum(xs)
}

func (qs QuorumSystem) IsWriteQuorum(xs map[GenericExpr]bool) bool {
	return qs.Writes.IsQuorum(xs)
}

func (qs QuorumSystem) Node(x string) Node {
	return qs.XtoNode[x]
}

func (qs QuorumSystem) Nodes() map[Node]bool {
	r := make(map[Node]bool, 0)

	for n := range qs.Reads.Nodes() {
		r[n] = true
	}

	for n := range qs.Writes.Nodes() {
		r[n] = true
	}

	return r
}

func (qs QuorumSystem) Elements() map[string]bool {
	r := make(map[string]bool, 0)

	for n := range qs.Nodes() {
		r[n.String()] = true
	}

	return r
}

func (qs QuorumSystem) Resilience() int {
	rr := qs.ReadResilience()
	ww := qs.WriteResilience()

	if rr < ww {
		return rr
	}

	return ww
}

func (qs QuorumSystem) ReadResilience() int {
	return qs.Reads.Resilience()
}

func (qs QuorumSystem) WriteResilience() int {
	return qs.Writes.Resilience()
}

func (qs QuorumSystem) DupFree() bool {
	return qs.Reads.DupFree() && qs.Writes.DupFree()
}

func (qs QuorumSystem) Strategy(opts ...func(options *StrategyOptions) error) (*Strategy, error) {

	sb := &StrategyOptions{}
	// ... (write initializations with default values)...
	for _, op := range opts {
		err := op(sb)
		if err != nil {
			return nil, err
		}
	}

	if sb.Optimize == Load && sb.LoadLimit != nil {
		return nil, fmt.Errorf("a load limit cannot be set when optimizing for load")
	}

	if sb.Optimize == Network && sb.NetworkLimit != nil {
		return nil, fmt.Errorf("a network limit cannot be set when optimizing for network")
	}

	if sb.Optimize == Latency && sb.LatencyLimit != nil {
		return nil, fmt.Errorf("a latency limit cannot be set when optimizing for latency")
	}

	if sb.F < 0 {
		return nil, fmt.Errorf("f must be >= 0")
	}

	if sb.F == 0 {
		return qs.loadOptimalStrategy()
	}

	d, err := canonicalizeRW(&sb.ReadFraction, &sb.WriteFraction)

	if err != nil {
		return nil, err
	}

	return qs.loadOptimalStrategy()
}
func (qs QuorumSystem) readQuorumLatency(quorum []Node) (*int, error) {
	return qs.quorumLatency(quorum, qs.IsReadQuorum)
}

func (qs QuorumSystem) writeQuorumLatency(quorum []Node) (*int, error) {
	return qs.quorumLatency(quorum, qs.IsWriteQuorum)
}

func (qs QuorumSystem) quorumLatency(quorum []Node, isQuorum func(map[GenericExpr]bool) bool) (*int, error) {

	sortedQ := make([]Node, 0)

	for q := range quorum {
		sortedQ = append(sortedQ, q)
	}

	nodeLatency := func(p1, p2 *Node) bool {
		return *p1.Latency < *p2.Latency
	}

	By(nodeLatency).Sort(sortedQ)

	for i := range quorum {
		xNodes := make(map[GenericExpr]bool)

		for _, q := range sortedQ[:i+1] {
			xNodes[q] = true
		}

		if isQuorum(xNodes) {
			return sortedQ[i].Latency, nil
		}
	}

	return nil, fmt.Errorf("_quorum_latency called on a non-quorum")

}

func (qs QuorumSystem) loadOptimalStrategy(
	readQuorums []map[Node]bool,
	writeQuorums []map[Node]bool,
	readFraction map[float64]float64) (*Strategy, error) {
	readQuorumVars := make([]lpVariable, 0)
	xToReadQuormmVars := make(map[Node][]lpVariable)

	for i, rq := range readQuorums {
		v := lpVariable{Name: fmt.Sprintf("r%b", i), UBound: 1, LBound: 1, Value: 1.0}
		readQuorumVars = append(readQuorumVars, v)

		for n := range rq {

			if _, ok := xToReadQuormmVars[n]; !ok {
				xToReadQuormmVars[n] = []lpVariable{v}
				continue
			}
			xToReadQuormmVars[n] = append(xToReadQuormmVars[n], v)
		}
	}

	writeQuorumVars := make([]lpVariable, 0)
	xToWriteQuorumVars := make(map[Node][]lpVariable)

	for i, rq := range writeQuorums {
		v := lpVariable{Name: fmt.Sprintf("r%b", i), UBound: 1, LBound: 1}
		writeQuorumVars = append(writeQuorumVars, v)

		for n, _ := range rq {
			if _, ok := xToWriteQuorumVars[n]; !ok {
				xToWriteQuorumVars[n] = []lpVariable{v}
				continue
			}
			xToWriteQuorumVars[n] = append(xToWriteQuorumVars[n], v)
		}
	}

	fr := 0.0

	for k, v := range readFraction {
		fr += k * v
	}

	network := func() float64 {
		reads := 0.0

		for i, v := range readQuorumVars {
			reads += v.Value * float64(len(readQuorums[i]))
		}

		reads = reads * fr

		writes := 0.0

		for i, v := range writeQuorumVars {
			reads += v.Value * float64(len(writeQuorums[i]))
		}

		writes = writes * (1 - fr)

		return reads + writes
	}

	latency := func() (float64, error) {

		reads := 0.0

		for i, v := range readQuorumVars {
			quorum := make([]Node, 0)

			for x := range readQuorums[i] {
				q := qs.Node(x.String())
				quorum = append(quorum, q)
			}

			l, err := qs.readQuorumLatency(quorum)

			if err != nil {
				return 0, err
			}

			reads += v.Value * float64(*l)
		}

		reads = reads * fr

		writes := 0.0

		for i, v := range writeQuorumVars {
			quorum := make([]Node, 0)

			for x := range writeQuorums[i] {
				q := qs.Node(x.String())
				quorum = append(quorum, q)
			}

			l, err := qs.writeQuorumLatency(quorum)

			if err != nil {
				return 0, err
			}

			reads += v.Value * float64(*l)
		}

		writes = writes * (1 - fr)

		return reads + writes, nil
	}

}

type lpVariable struct {
	Name   string
	Value  float64
	UBound int
	LBound int
}

type Strategy struct {
}

type NodeSorter struct {
	nodes []Node
	by    func(p1, p2 *Node) bool // Closure used in the Less method.
}

// Len is part of sort.Interface.
func (ns *NodeSorter) Len() int {
	return len(ns.nodes)
}

// Swap is part of sort.Interface.
func (ns *NodeSorter) Swap(i, j int) {
	ns.nodes[i], ns.nodes[j] = ns.nodes[j], ns.nodes[i]
}

// Less is part of sort.Interface. It is implemented by calling the "by" closure in the sorter.
func (ns *NodeSorter) Less(i, j int) bool {
	return ns.by(&ns.nodes[i], &ns.nodes[j])
}

type By func(p1, p2 *Node) bool

// Sort is a method on the function type, By, that sorts the argument slice according to the function.
func (by By) Sort(nodes []Node) {
	ps := &NodeSorter{
		nodes: nodes,
		by:    by, // The Sort method's receiver is the function (closure) that defines the sort order.
	}
	sort.Sort(ps)
}
