package pkg

import (
	"fmt"
	"github.com/lanl/clp"
	wr "github.com/mroth/weightedrand"
	"math"
	"math/rand"
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

type lpVariable struct {
	Name   string
	Value  float64
	UBound int
	LBound int
	Index  int
}

func DefQuorumSystem(reads GenericExpr, writes GenericExpr) (QuorumSystem, error) {
	optionalWrites := reads.Dual()

	for k := range writes.Quorums() {
		if !optionalWrites.IsQuorum(k) {
			return QuorumSystem{}, fmt.Errorf("Not all read quorums intersect all write quorums")
		}
	}

	return QuorumSystem{Reads: reads, Writes: writes}, nil

}

func DefQuorumSystemWithReads(reads GenericExpr) QuorumSystem {
	return QuorumSystem{Reads: reads, Writes: reads.Dual()}
}

func DefQuorumSystemWithWrites(writes GenericExpr) QuorumSystem {
	return QuorumSystem{Reads: writes.Dual(), Writes: writes}
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

func (qs QuorumSystem) Nodes() NodeSet {
	r := make(NodeSet, 0)

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

func (qs QuorumSystem) Resilience() uint {
	rr := qs.ReadResilience()
	ww := qs.WriteResilience()

	if rr < ww {
		return rr
	}

	return ww
}

func (qs QuorumSystem) ReadResilience() uint {
	return qs.Reads.Resilience()
}

func (qs QuorumSystem) WriteResilience() uint {
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

	rq := make([]map[GenericExpr]bool, 0)
	wq := make([]map[GenericExpr]bool, 0)

	for e := range qs.ReadQuorums() {
		rq = append(rq, e)
	}

	for e := range qs.WriteQuorums() {
		wq = append(rq, e)
	}

	d, err := canonicalizeRW(&sb.ReadFraction, &sb.WriteFraction)

	if err != nil {
		return nil, err
	}

	if sb.F == 0 {
		return qs.loadOptimalStrategy(sb.Optimize, rq, wq, d,
			sb.LoadLimit, sb.NetworkLimit, sb.LatencyLimit)
	}

	// TODO: implement resilience
	return qs.loadOptimalStrategy(sb.Optimize, rq, wq, sb.ReadFraction.GetValue(),
		sb.LoadLimit, sb.NetworkLimit, sb.LatencyLimit)
}

func (qs QuorumSystem) UniformStrategy(f int) (Strategy, error) {

	readQuorums := make([]ExprSet, 0)
	writeQuorums := make([]ExprSet, 0)

	if f < 0 {
		return Strategy{}, fmt.Errorf("f must be >= 0")
	} else if f == 0 {
		for q := range qs.ReadQuorums() {
			readQuorums = append(readQuorums, q)
		}

		for q := range qs.WriteQuorums() {
			writeQuorums = append(writeQuorums, q)
		}
	}

	readQuorums = qs.minimize(readQuorums)
	writeQuorums = qs.minimize(writeQuorums)

	sigmaR := make([]SigmaRecord, 0)
	sigmaW := make([]SigmaRecord, 0)

	for _, q := range readQuorums {
		sigmaR = append(sigmaR, SigmaRecord{q, 1 / float64(len(readQuorums))})
	}

	for _, q := range writeQuorums {
		sigmaW = append(sigmaW, SigmaRecord{q, 1 / float64(len(writeQuorums))})
	}

	return Strategy{SigmaR: Sigma{sigmaR}, SigmaW: Sigma{sigmaW}}, nil
}

func (qs QuorumSystem) MakeStrategy(sigmaR Sigma, sigmaW Sigma) (Strategy, error) {
	normalizedSigmaR := make([]SigmaRecord, 0)
	normalizedSigmaW := make([]SigmaRecord, 0)

	allCheck := func(records []SigmaRecord, checkCondition func(record SigmaRecord) bool) bool {
		for _, r := range records {
			if !checkCondition(r) {
				return false
			}
		}
		return true
	}

	if !allCheck(sigmaR.Values, func(r SigmaRecord) bool { return r.Probability >= 0 }) {
		return Strategy{}, fmt.Errorf("SigmaR has negative weights")
	}

	if !allCheck(sigmaW.Values, func(r SigmaRecord) bool { return r.Probability >= 0 }) {
		return Strategy{}, fmt.Errorf("SigmaW has negative weights")
	}

	if !allCheck(sigmaR.Values, func(r SigmaRecord) bool { return qs.IsReadQuorum(r.Quorum) }) {
		return Strategy{}, fmt.Errorf("SigmaR has non-read quorums")
	}

	if !allCheck(sigmaW.Values, func(w SigmaRecord) bool { return qs.IsWriteQuorum(w.Quorum) }) {
		return Strategy{}, fmt.Errorf("SigmaW has non-write quorums")
	}

	totalSigmaR := 0.0

	for _, value := range sigmaR.Values {
		totalSigmaR += value.Probability
	}

	totalSigmaW := 0.0

	for _, value := range sigmaW.Values {
		totalSigmaW += value.Probability
	}

	for _, value := range sigmaR.Values {
		normalizedSigmaR = append(normalizedSigmaR,
			SigmaRecord{Quorum: value.Quorum, Probability: value.Probability / totalSigmaR})
	}

	for _, value := range sigmaW.Values {
		normalizedSigmaW = append(normalizedSigmaW,
			SigmaRecord{Quorum: value.Quorum, Probability: value.Probability / totalSigmaW})
	}

	return Strategy{SigmaR: Sigma{Values: normalizedSigmaR}, SigmaW: Sigma{Values: normalizedSigmaW}}, nil
}

func (qs QuorumSystem) minimize(sets []ExprSet) []ExprSet {

	sort.Slice(sets, func(i, j int) bool {
		return len(sets[i]) < len(sets[j])
	})

	isSuperSet := func(x ExprSet, e ExprSet) bool {
		set := make(map[GenericExpr]int)
		for k := range x {
			set[k] += 1
		}

		for k := range e {
			if count, found := set[k]; !found {
				return false
			} else if count < 1 {
				return false
			} else {
				set[k] = count - 1
			}
		}

		return true
	}

	isAnySuperSet := func(x ExprSet, e []ExprSet) bool {
		for _, expr := range e {
			if isSuperSet(x, expr) {
				return true
			}
		}

		return false
	}

	minimalElements := make([]ExprSet, 0)

	for _, v := range sets {
		if !(isAnySuperSet(v, minimalElements)) {
			minimalElements = append(minimalElements, v)
		}
	}

	return minimalElements
}

func (qs QuorumSystem) fResilientQuorums(f int, xs []Node, e GenericExpr) {

}

func (qs QuorumSystem) readQuorumLatency(quorum []Node) (*int, error) {
	return qs.quorumLatency(quorum, qs.IsReadQuorum)
}

func (qs QuorumSystem) writeQuorumLatency(quorum []Node) (*int, error) {
	return qs.quorumLatency(quorum, qs.IsWriteQuorum)
}

func (qs QuorumSystem) quorumLatency(quorum []Node, isQuorum func(map[GenericExpr]bool) bool) (*int, error) {

	sortedQ := make([]Node, 0)

	for _, q := range quorum {
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
	readQuorums []map[GenericExpr]bool,
	writeQuorums []map[GenericExpr]bool,
	readFraction map[float64]float64,
	loadLimit *float64,
	networkLimit *float64,
	latencyLimit *float64) (*Strategy, error) {

	readQuorumVars := make([]lpVariable, 0)
	xToReadQuorumVars := make(map[GenericExpr][]lpVariable)

	for i, rq := range readQuorums {
		v := lpVariable{Name: fmt.Sprintf("r%b", i), UBound: 1, LBound: 1, Value: 1.0}
		readQuorumVars = append(readQuorumVars, v)

		for n := range rq {

			if _, ok := xToReadQuorumVars[n]; !ok {
				xToReadQuorumVars[n] = []lpVariable{v}
				continue
			}
			xToReadQuorumVars[n] = append(xToReadQuorumVars[n], v)
		}
	}

	writeQuorumVars := make([]lpVariable, 0)
	xToWriteQuorumVars := make(map[GenericExpr][]lpVariable)

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

	network := func(networkLimit *float64) ([]float64, [][2]float64, [][]float64) {
		vars := make([]float64, 0)
		constr := make([][2]float64, 0)
		obj := make([][]float64, 0)
		ninf := math.Inf(-1)
		pinf := math.Inf(1)

		// initializes target array
		for _, v := range readQuorumVars {
			vars = append(vars, 1.0)
			b := [][2]float64{{float64(v.LBound)}, {float64(v.UBound)}}
			constr = append(constr, b...)

		}
		// add constraints 0 <= q <= 1
		for _, v := range writeQuorumVars {
			vars = append(vars, 1.0)
			b := [][2]float64{{float64(v.LBound)}, {float64(v.UBound)}}
			constr = append(constr, b...)
		}

		tmp := make([]float64, 0)
		tmp = append(tmp, ninf)

		// network_def  - inf <= network_def <= +inf
		for i := range readQuorumVars {
			tmp = append(tmp, fr*float64(len(readQuorums[i])))
		}

		for i := range writeQuorumVars {
			tmp = append(tmp, (1-fr)*float64(len(writeQuorums[i])))
		}

		if networkLimit == nil {
			tmp = append(tmp, pinf)
		} else {
			tmp = append(tmp, *networkLimit)
		}
		obj = append(obj, tmp)

		return vars, constr, obj
	}

	latency := func(latencyLimit *float64) ([]float64, [][2]float64, [][]float64, error) {
		vars := make([]float64, 0)
		constr := make([][2]float64, 0)
		obj := make([][]float64, 0)
		ninf := math.Inf(-1)
		pinf := math.Inf(1)

		// initializes target array
		for _, v := range readQuorumVars {
			vars = append(vars, 1.0)
			b := [][2]float64{{float64(v.LBound)}, {float64(v.UBound)}}
			constr = append(constr, b...)

		}
		// add constraints 0 <= q <= 1
		for _, v := range writeQuorumVars {
			vars = append(vars, 1.0)
			b := [][2]float64{{float64(v.LBound)}, {float64(v.UBound)}}
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
			tmp = append(tmp, fr*v.Value*float64(*l))
		}

		for i, v := range writeQuorumVars {
			quorum := make([]Node, 0)

			for x := range writeQuorums[i] {
				q := qs.Node(x.String())
				quorum = append(quorum, q)
			}

			l, _ := qs.writeQuorumLatency(quorum)

			tmp = append(tmp, (1-fr)*v.Value*float64(*l))
		}

		if latencyLimit == nil {
			tmp = append(tmp, pinf)
		} else {
			tmp = append(tmp, *latencyLimit)
		}
		obj = append(obj, tmp)

		return vars, constr, obj, nil
	}

	frLoad := func(loadLimit *float64) ([]float64, [][2]float64, [][]float64, error) {
		vars := make([]float64, 0)
		constr := make([][2]float64, 0)
		obj := make([][]float64, 0)
		ninf := math.Inf(-1)
		pinf := math.Inf(1)

		// initializes target array
		for _, v := range readQuorumVars {
			vars = append(vars, 1.0)
			b := [][2]float64{{float64(v.LBound)}, {float64(v.UBound)}}
			constr = append(constr, b...)

		}
		// add constraints 0 <= q <= 1
		for _, v := range writeQuorumVars {
			vars = append(vars, 1.0)
			b := [][2]float64{{float64(v.LBound)}, {float64(v.UBound)}}
			constr = append(constr, b...)
		}

		// l def
		vars = append(vars, 1.0)
		b := [][2]float64{{ninf}, {pinf}}
		constr = append(constr, b...)

		tmp := make([]float64, 0)
		tmp = append(tmp, ninf)

		for n := range qs.Nodes() {
			if _, ok := xToReadQuorumVars[n]; ok {
				vs := xToReadQuorumVars[n]

				for _, v := range vs {
					tmp = append(tmp, fr*v.Value / *qs.Node(n.Name).ReadCapacity)
				}
			}
		}

		for n := range qs.Nodes() {
			if _, ok := xToWriteQuorumVars[n]; ok {
				vs := xToWriteQuorumVars[n]

				for _, v := range vs {
					tmp = append(tmp, (1-fr)*v.Value / *qs.Node(n.Name).WriteCapacity)
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

	vars := make([]float64, 0)
	constr := make([][2]float64, 0)
	obj := make([][]float64, 0)

	obj = append(obj, readQConstraint)
	obj = append(obj, writeQConstraint)

	if optimize == Load {
		vars, constr, obj, _ = frLoad(nil)
	} else if optimize == Network {
		vars, constr, obj = network(nil)
	} else if optimize == Latency {
		vars, constr, obj, _ = latency(nil)
	}

	if latencyLimit != nil {
		_, _, lobj, _ := latency(latencyLimit)
		obj = append(obj, lobj[0])
	}

	if networkLimit != nil {
		_, _, lobj, _ := latency(latencyLimit)
		obj = append(obj, lobj[0])
	}

	if loadLimit != nil {
		_, _, lobj, _ := frLoad(loadLimit)
		obj = append(obj, lobj[0])
	}

	simp.EasyLoadDenseProblem(vars, constr, obj)
	// Solve the optimization problem.
	simp.Primal(clp.NoValuesPass, clp.NoStartFinishOptions)
	soln := simp.PrimalColumnSolution()

	fmt.Println(soln)

	return &Strategy{}, nil
}

//Strategy
type Strategy struct {
	Qs                QuorumSystem
	SigmaR            Sigma
	SigmaW            Sigma
	XReadProbability  map[Node]float64
	XWriteProbability map[Node]float64
}

type SigmaRecord struct {
	Quorum      ExprSet
	Probability Probability
}
type Sigma struct {
	Values []SigmaRecord
}

func (s Strategy) String() string {
	return ""
}

func (s Strategy) GetReadQuorum() map[GenericExpr]bool {

	rand.Seed(time.Now().UTC().UnixNano()) // always seed random!

	criteria := make([]wr.Choice, 0)

	for _, sigmaRecord := range s.SigmaR.Values{
		criteria = append(criteria, wr.Choice{ sigmaRecord.Quorum, uint(sigmaRecord.Probability * 10)})
	}

	chooser, _ := wr.NewChooser(criteria...)
	result := chooser.Pick().(ExprSet)

	return result
}

func (s Strategy) GetWriteQuorum() map[GenericExpr]bool {

	rand.Seed(time.Now().UTC().UnixNano()) // always seed random!

	criteria := make([]wr.Choice, 0)

	for _, sigmaRecord := range s.SigmaW.Values{
		criteria = append(criteria, wr.Choice{ sigmaRecord.Quorum, uint(sigmaRecord.Probability * 10)})
	}

	chooser, _ := wr.NewChooser(criteria...)
	result := chooser.Pick().(ExprSet)

	return result
}

func (s Strategy) Load(rf *Distribution, wf *Distribution) (*float64, error) {
	d, err := canonicalizeRW(rf, wf)
	if err != nil {
		return nil, err
	}

	sum := 0.0

	for fr, p := range d {
		sum += p * s.maxload(fr)
	}
	return &sum, nil
}

func (s Strategy) Capacity(rf *Distribution, wf *Distribution) (*float64, error) {
	d, err := canonicalizeRW(rf, wf)
	if err != nil {
		return nil, err
	}

	sum := 0.0

	for fr, p := range d {
		sum += p * 1.0 / s.maxload(fr)
	}
	return &sum, nil
}

func (s Strategy) NetworkLoad(rf *Distribution, wf *Distribution) (*float64, error) {
	d, err := canonicalizeRW(rf, wf)
	if err != nil {
		return nil, err
	}

	frsum := 0.0

	for fr, p := range d {
		frsum += p * fr
	}

	reads := 0.0
	for _, sigma := range s.SigmaR.Values {
		reads += frsum * float64(len(sigma.Quorum)) * sigma.Probability
	}

	writes := 0.0
	for _, sigma := range s.SigmaW.Values {
		writes += (1 - frsum) * float64(len(sigma.Quorum)) * sigma.Probability
	}

	total := reads + writes
	return &total, nil
}

func (s Strategy) Latency(rf *Distribution, wf *Distribution) (*float64, error) {
	d, err := canonicalizeRW(rf, wf)
	if err != nil {
		return nil, err
	}

	frsum := 0.0

	for fr, p := range d {
		frsum += p * fr
	}

	reads := 0.0

	for _, rq := range s.SigmaR.Values {
		nodes := make([]Node, 0)

		for n := range rq.Quorum {
			nodes = append(nodes, s.Qs.Node(n.String()))
		}

		v, _ := s.Qs.readQuorumLatency(nodes)
		reads += float64(*v) * rq.Probability
	}

	writes := 0.0

	for _, wq := range s.SigmaW.Values {
		nodes := make([]Node, 0)

		for n := range wq.Quorum {
			nodes = append(nodes, s.Qs.Node(n.String()))
		}

		v, _ := s.Qs.writeQuorumLatency(nodes)
		writes += float64(*v) * wq.Probability
	}

	total := reads + writes
	return &total, nil
}

func (s Strategy) NodeLoad(node Node, rf *Distribution, wf *Distribution) (*float64, error) {
	d, err := canonicalizeRW(rf, wf)
	if err != nil {
		return nil, err
	}

	sum := 0.0

	for fr, p := range d {
		sum += p * s.nodeLoad(node, fr)
	}
	return &sum, nil
}

func (s Strategy) NodeUtilization(node Node, rf *Distribution, wf *Distribution) (*float64, error) {
	d, err := canonicalizeRW(rf, wf)
	if err != nil {
		return nil, err
	}

	sum := 0.0

	for fr, p := range d {
		sum += p * s.nodeUtilization(node, fr)
	}
	return &sum, nil
}

func (s Strategy) NodeThroughput(node Node, rf *Distribution, wf *Distribution) (*float64, error) {
	d, err := canonicalizeRW(rf, wf)
	if err != nil {
		return nil, err
	}

	sum := 0.0

	for fr, p := range d {
		sum += p * s.nodeThroughput(node, fr)
	}
	return &sum, nil
}

func (s Strategy) maxload(fr float64) float64 {
	max := 0.0

	for n := range s.Qs.Nodes() {
		if s.nodeLoad(n, fr) > max {
			max = s.nodeLoad(n, fr)
		}
	}

	return max
}

func (s Strategy) nodeLoad(node Node, fr float64) float64 {
	fw := 1 - fr
	return fr*s.XReadProbability[node] / *node.ReadCapacity +
		fw*s.XWriteProbability[node] / *node.WriteCapacity
}

func (s Strategy) nodeUtilization(node Node, fr float64) float64 {
	return s.nodeLoad(node, fr) / s.maxload(fr)
}

func (s Strategy) nodeThroughput(node Node, fr float64) float64 {
	capacity := 1 / s.maxload(fr)
	fw := 1 - fr

	return capacity * (fr*s.XReadProbability[node] + fw*s.XWriteProbability[node])
}

// Sorter
type nodeSorter struct {
	nodes []Node
	by    func(p1, p2 *Node) bool // Closure used in the Less method.
}

// Len is part of sort.Interface.
func (ns *nodeSorter) Len() int {
	return len(ns.nodes)
}

// Swap is part of sort.Interface.
func (ns *nodeSorter) Swap(i, j int) {
	ns.nodes[i], ns.nodes[j] = ns.nodes[j], ns.nodes[i]
}

// Less is part of sort.Interface. It is implemented by calling the "by" closure in the sorter.
func (ns *nodeSorter) Less(i, j int) bool {
	return ns.by(&ns.nodes[i], &ns.nodes[j])
}

type By func(p1, p2 *Node) bool

// Sort is a method on the function type, By, that sorts the argument slice according to the function.
func (by By) Sort(nodes []Node) {
	ps := &nodeSorter{
		nodes: nodes,
		by:    by, // The Sort method's receiver is the function (closure) that defines the sort order.
	}
	sort.Sort(ps)
}
