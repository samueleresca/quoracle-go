package pkg

import (
	"fmt"
	"github.com/lanl/clp"
	"math"
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
	optimize OptimizeType,
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

	network := func() ([]float64, [][2]float64, [][]float64) {
		vars := make([]float64, 0)
		constr := make([][2]float64, 0)
		obj := make([][]float64, 0)
		ninf := math.Inf(-1)
		pinf := math.Inf(1)

		// initializes target array
		for _, v := range readQuorumVars {
			vars = append(vars, 1.0)
			b := [][2]float64{{float64(v.LBound)},{float64(v.UBound)}}
			constr = append(constr, b...)

		}
		// add constraints 0 <= q <= 1
		for _, v := range writeQuorumVars {
			vars = append(vars, 1.0)
			b := [][2]float64{{float64(v.LBound)},{float64(v.UBound)}}
			constr = append(constr, b...)
		}

		tmp := make([]float64, 0)
		tmp = append(tmp, ninf)

		// network_def  - inf <= network_def <= +inf
		for i := range readQuorumVars {
			tmp = append(tmp, fr * float64(len(readQuorums[i])))
		}

		for i := range writeQuorumVars {
			tmp = append(tmp, (1 - fr) * float64(len(writeQuorums[i])))
		}

		tmp = append(tmp, pinf)
		obj = append(obj, tmp)

		return vars, constr, obj
	}

	latency := func() ([]float64, [][2]float64, [][]float64, error)  {
		vars := make([]float64, 0)
		constr := make([][2]float64, 0)
		obj := make([][]float64, 0)
		ninf := math.Inf(-1)
		pinf := math.Inf(1)

		// initializes target array
		for _, v := range readQuorumVars {
			vars = append(vars, 1.0)
			b := [][2]float64{{float64(v.LBound)},{float64(v.UBound)}}
			constr = append(constr, b...)

		}
		// add constraints 0 <= q <= 1
		for _, v := range writeQuorumVars {
			vars = append(vars, 1.0)
			b := [][2]float64{{float64(v.LBound)},{float64(v.UBound)}}
			constr = append(constr, b...)
		}

		// building latency objs   -inf <= latency_def <= inf

		tmp := make([]float64, 0)
		tmp = append(tmp, ninf)

		for i, v := range readQuorumVars {
			quorum := make([]Node, 0)

			for x := range readQuorums[i] {
				q := qs.Node(x.String())
				quorum = append(quorum, q)
			}

			l, _ := qs.readQuorumLatency(quorum)
			tmp = append(tmp, fr * v.Value * float64(*l))
		}

		for i, v := range writeQuorumVars {
			quorum := make([]Node, 0)

			for x := range writeQuorums[i] {
				q := qs.Node(x.String())
				quorum = append(quorum, q)
			}

			l, _ := qs.writeQuorumLatency(quorum)

			tmp = append(tmp, (1 - fr) * v.Value * float64(*l))
		}

		tmp = append(tmp, pinf)
		obj = append(obj, tmp)

		return vars, constr, obj, nil
	}

	fr_load := func() ([]float64, [][2]float64, [][]float64, error){
		vars := make([]float64, 0)
		constr := make([][2]float64, 0)
		obj := make([][]float64, 0)
		ninf := math.Inf(-1)
		pinf := math.Inf(1)

		// initializes target array
		for _, v := range readQuorumVars {
			vars = append(vars, 1.0)
			b := [][2]float64{{float64(v.LBound)},{float64(v.UBound)}}
			constr = append(constr, b...)

		}
		// add constraints 0 <= q <= 1
		for _, v := range writeQuorumVars {
			vars = append(vars, 1.0)
			b := [][2]float64{{float64(v.LBound)},{float64(v.UBound)}}
			constr = append(constr, b...)
		}

		// l def
		vars = append(vars, 1.0)
		b := [][2]float64{{ninf},{ pinf}}
		constr = append(constr, b...)

		tmp := make([]float64, 0)
		tmp = append(tmp, ninf)

		for n := range qs.Nodes(){
			if _, ok := xToReadQuormmVars[n]; ok {
				vs := xToReadQuormmVars[n]

				for _, v := range vs {
					tmp = append(tmp, fr * v.Value /  *qs.Node(n.Name).ReadCapacity)
				}
			}
		}

		for n := range qs.Nodes() {
			if _, ok := xToWriteQuorumVars[n]; ok {
				vs := xToWriteQuorumVars[n]

				for _, v := range vs {
					tmp = append(tmp, (1-fr) * v.Value / *qs.Node(n.Name).WriteCapacity)
				}
			}
		}

		tmp = append(tmp, -1.0)
		tmp = append(tmp, 0)

		obj = append(obj, tmp)


		return vars, constr, obj, nil
	}

	simp := clp.NewSimplex()
	simp.SetOptimizationDirection(clp.Minimize)

	// read quorum constraint
	readQConstraint := make([]float64, 0)
	readQConstraint = append(readQConstraint, 1)

	for range readQuorumVars {
		readQConstraint = append(readQConstraint, 1.0)
	}

	for range writeQuorumVars {
		readQConstraint = append(readQConstraint, 0.0)
	}

	readQConstraint = append(readQConstraint, 1)

	// write quorum constraint
	writeQConstraint := make([]float64, 0)
	writeQConstraint = append(writeQConstraint, 1)

	for range readQuorumVars {
		writeQConstraint = append(writeQConstraint, 0.0)
	}

	for range writeQuorumVars {
		readQConstraint = append(writeQConstraint, 1.0)
	}

	writeQConstraint = append(writeQConstraint, 1)

	if optimize == Load {
		vars, constr, obj, _ := fr_load()

		obj = append(obj, readQConstraint)
		obj = append(obj, writeQConstraint)

		simp.EasyLoadDenseProblem(vars, constr, obj)
	} else if  optimize == Network {
		vars, constr, obj := network()

		obj = append(obj, readQConstraint)
		obj = append(obj, writeQConstraint)

		simp.EasyLoadDenseProblem(vars, constr, obj)
	} else if optimize == Latency {
		vars, constr, obj, _ := latency()

		obj = append(obj, readQConstraint)
		obj = append(obj, writeQConstraint)

		simp.EasyLoadDenseProblem(vars, constr, obj)
	}


	// Solve the optimization problem.
	simp.Primal(clp.NoValuesPass, clp.NoStartFinishOptions)
	soln := simp.PrimalColumnSolution()

	fmt.Println(soln)

}

type lpVariable struct {
	Name   string
	Value  float64
	UBound int
	LBound int
	Index  int
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
