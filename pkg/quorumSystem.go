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
	UBound float64
	LBound float64
	Index  int
	Quorum *ExprSet
}

func DefQuorumSystem(reads GenericExpr, writes GenericExpr) (QuorumSystem, error) {
	optionalWrites := reads.Dual()

	for k := range writes.Quorums() {
		if !optionalWrites.IsQuorum(k) {
			return QuorumSystem{}, fmt.Errorf("Not all read quorums intersect all write quorums")
		}
	}
	qs := QuorumSystem{Reads: reads, Writes: writes}

	qs.XtoNode = map[string]Node{}

	for node := range qs.Nodes() {
		qs.XtoNode[node.Name] = node
	}

	return qs, nil
}

func DefQuorumSystemWithReads(reads GenericExpr) QuorumSystem {
	qs := QuorumSystem{Reads: reads, Writes: reads.Dual()}

	qs.XtoNode = map[string]Node{}

	for node := range qs.Nodes() {
		qs.XtoNode[node.Name] = node
	}

	return qs
}

func DefQuorumSystemWithWrites(writes GenericExpr) QuorumSystem {
	qs := QuorumSystem{Reads: writes.Dual(), Writes: writes}

	qs.XtoNode = map[string]Node{}

	for node := range qs.Nodes() {
		qs.XtoNode[node.Name] = node
	}

	return qs
}

func (qs QuorumSystem) String() string {
	return ""
}

func initStrategyOptions(initOptions StrategyOptions) func(options *StrategyOptions) error {
	init := func(options *StrategyOptions) error {
		options.Optimize = initOptions.Optimize
		options.LatencyLimit = initOptions.LatencyLimit
		options.NetworkLimit = initOptions.NetworkLimit
		options.LoadLimit = initOptions.LoadLimit
		options.F = initOptions.F
		options.ReadFraction = initOptions.ReadFraction
		options.WriteFraction = initOptions.WriteFraction

		return nil
	}
	return init
}

func (qs QuorumSystem) Capacity(strategyOptions StrategyOptions) (*float64, error) {

	strategy, err := qs.Strategy(initStrategyOptions(strategyOptions))

	if err != nil {
		return nil, err
	}

	return strategy.Capacity(&strategyOptions.ReadFraction, &strategyOptions.WriteFraction)
}

func (qs QuorumSystem) Latency(strategyOptions StrategyOptions) (*float64, error) {

	strategy, err := qs.Strategy(initStrategyOptions(strategyOptions))

	if err != nil {
		return nil, err
	}

	return strategy.Latency(&strategyOptions.ReadFraction, &strategyOptions.WriteFraction)
}

func (qs QuorumSystem) Load(strategyOptions StrategyOptions) (*float64, error) {

	strategy, err := qs.Strategy(initStrategyOptions(strategyOptions))

	if err != nil {
		return nil, err
	}

	return strategy.Load(&strategyOptions.ReadFraction, &strategyOptions.WriteFraction)
}

func (qs QuorumSystem) NetworkLoad(strategyOptions StrategyOptions) (*float64, error) {

	strategy, err := qs.Strategy(initStrategyOptions(strategyOptions))

	if err != nil {
		return nil, err
	}

	return strategy.NetworkLoad(&strategyOptions.ReadFraction, &strategyOptions.WriteFraction)
}
func (qs QuorumSystem) ReadQuorums() chan ExprSet {
	return qs.Reads.Quorums()
}

func (qs QuorumSystem) WriteQuorums() chan ExprSet {
	for t := range qs.Writes.Quorums() {
		fmt.Println(t)
	}
	return qs.Writes.Quorums()
}

func (qs QuorumSystem) IsReadQuorum(xs ExprSet) bool {
	return qs.Reads.IsQuorum(xs)
}

func (qs QuorumSystem) IsWriteQuorum(xs ExprSet) bool {
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

func (qs QuorumSystem) Elements() []Node {
	nodes := make([]Node, 0)
	for n := range qs.Nodes() {
		nodes = append(nodes, n)
	}
	return nodes
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

	rq := make([]ExprSet, 0)
	wq := make([]ExprSet, 0)

	for e := range qs.ReadQuorums() {
		rq = append(rq, e)
	}

	for e := range qs.WriteQuorums() {
		wq = append(wq, e)
	}

	d, err := canonicalizeRW(&sb.ReadFraction, &sb.WriteFraction)

	if err != nil {
		return nil, err
	}

	if sb.F == 0 {
		return qs.loadOptimalStrategy(sb.Optimize, rq, wq, d,
			sb.LoadLimit, sb.NetworkLimit, sb.LatencyLimit)
	}

	xs := qs.Elements()

	rq = make([]ExprSet, 0)
	wq = make([]ExprSet, 0)

	for _, e := range qs.fResilientQuorums(sb.F, xs, qs.Reads) {
		rq = append(rq, e)
	}

	for _, e := range qs.fResilientQuorums(sb.F, xs, qs.Writes) {
		wq = append(wq, e)
	}

	return qs.loadOptimalStrategy(sb.Optimize, rq, wq, d,
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

	return DefStrategy(qs, Sigma{Values: normalizedSigmaR}, Sigma{Values: normalizedSigmaW}), nil
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

func (qs QuorumSystem) fResilientQuorums(f int, xs []Node, e GenericExpr) []ExprSet {
	s := ExprSet{}
	result := make([]ExprSet, 0)
	return fResilientHelper(result, f, xs, e, s, 0)
}

func fResilientHelper(result []ExprSet, f int, xs []Node, e GenericExpr, s ExprSet, i int) []ExprSet {
	minf := f

	if f > len(s) {
		minf = len(s)
	}

	isAll := true
	combinationSets := combinations(exprMapToList(s), minf)

	for _, failure := range combinationSets {
		if !e.IsQuorum(removeFromExprSet(s, failure)) {
			isAll = false
		}
	}

	if isAll && len(combinationSets) > 0 {
		result = append(result, s)
		return result
	}

	for j := i; j < len(xs); j++ {
		s[xs[j]] = true
		defer delete(s, xs[j])
		return fResilientHelper(result, f, xs, e, copyExprSet(s), j+1)

	}
	return result
}

func removeFromExprSet(set ExprSet, g []GenericExpr) ExprSet {
	newSet := copyExprSet(set)

	for _, e := range g {
		delete(newSet, e)
	}

	return newSet
}

func copyExprSet(set ExprSet) ExprSet {
	newSet := make(ExprSet)

	for k, v := range set {
		newSet[k] = v
	}
	return newSet
}

func (qs QuorumSystem) readQuorumLatency(quorum []Node) (*uint, error) {
	return qs.quorumLatency(quorum, qs.IsReadQuorum)
}

func (qs QuorumSystem) writeQuorumLatency(quorum []Node) (*uint, error) {
	return qs.quorumLatency(quorum, qs.IsWriteQuorum)
}

func (qs QuorumSystem) quorumLatency(quorum []Node, isQuorum func(set ExprSet) bool) (*uint, error) {

	sortedQ := make([]Node, 0)

	for _, q := range quorum {
		sortedQ = append(sortedQ, q)
	}

	nodeLatency := func(p1, p2 *Node) bool {
		return *p1.Latency < *p2.Latency
	}

	By(nodeLatency).Sort(sortedQ)

	for i := range quorum {
		xNodes := make(ExprSet)

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
	readQuorums []ExprSet,
	writeQuorums []ExprSet,
	readFraction map[float64]float64,
	loadLimit *float64,
	networkLimit *float64,
	latencyLimit *float64) (*Strategy, error) {

	readQuorumVars, xToReadQuorumVars, writeQuorumVars, xToWriteQuorumVars := defineOptimizationVars(readQuorums, writeQuorums)

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
			b := [2]float64{v.LBound, v.UBound}
			constr = append(constr, b)

		}
		// add constraints 0 <= q <= 1
		for _, v := range writeQuorumVars {
			vars = append(vars, 1.0)
			b := [2]float64{v.LBound, v.UBound}
			constr = append(constr, b)
		}

		tmp := make([]float64, len(vars))

		// network_def  - inf <= network_def <= +inf
		for _, v := range readQuorumVars {
			tmp[v.Index] = fr * float64(len(*v.Quorum))
		}

		for _, v := range writeQuorumVars {
			tmp[v.Index] = (1 - fr) * float64(len(*v.Quorum))
		}

		tmp = append([]float64{ninf}, tmp...)

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

		// initializes vars array
		for range readQuorumVars {
			vars = append(vars, 1.0)
		}

		for range writeQuorumVars {
			vars = append(vars, 1.0)
		}
		// add constraints 0 <= q <= 1

		for _, v := range readQuorumVars {
			b := [2]float64{v.LBound, v.UBound}
			constr = append(constr, b)
		}

		for _, v := range writeQuorumVars {
			b := [2]float64{v.LBound, v.UBound}
			constr = append(constr, b)
		}

		// building latency objs | -inf <= latency_def <= inf

		tmp := make([]float64, len(vars))

		for _, v := range readQuorumVars {
			quorum := make([]Node, 0)

			for x := range *v.Quorum {
				q := qs.Node(x.String())
				quorum = append(quorum, q)
			}

			l, _ := qs.readQuorumLatency(quorum)
			tmp[v.Index] = fr * v.Value * float64(*l)
		}

		for _, v := range writeQuorumVars {
			quorum := make([]Node, 0)

			for x := range *v.Quorum {
				q := qs.Node(x.String())
				quorum = append(quorum, q)
			}

			l, _ := qs.writeQuorumLatency(quorum)
			tmp[v.Index] = (1 - fr) * v.Value * float64(*l)
		}

		tmp = append([]float64{ninf}, tmp...)

		if latencyLimit == nil {
			tmp = append(tmp, pinf)
		} else {
			tmp = append(tmp, *latencyLimit)
		}
		obj = append(obj, tmp)

		return vars, constr, obj, nil
	}

	frLoad := func(loadLimit *float64, fr float64) ([]float64, [][2]float64, [][]float64, error) {
		vars := make([]float64, 0)
		constr := make([][2]float64, 0)
		ninf := math.Inf(-1)
		pinf := math.Inf(1)

		// initializes target array
		for _, v := range readQuorumVars {
			vars = append(vars, 1.0)
			b := [2]float64{v.LBound, v.UBound}
			constr = append(constr, b)

		}
		// add constraints 0 <= q <= 1
		for _, v := range writeQuorumVars {
			vars = append(vars, 1.0)
			b := [2]float64{v.LBound, v.UBound}
			constr = append(constr, b)
		}

		// l def
		vars = append(vars, 1.0)
		b := [2]float64{ninf, pinf}
		constr = append(constr, b)

		// Load formula
		objectives := make([][]float64, 0)
		for n := range qs.Nodes() {
			tmp := make([]float64, len(vars))

			if _, ok := xToReadQuorumVars[n]; ok {
				vs := xToReadQuorumVars[n]
				for _, v := range vs {
					tmp[v.Index] += fr * v.Value / float64(*qs.Node(n.Name).ReadCapacity)
				}
			}

			if _, ok := xToWriteQuorumVars[n]; ok {
				vs := xToWriteQuorumVars[n]
				for _, v := range vs {
					tmp[v.Index] += (1 - fr) * v.Value / float64(*qs.Node(n.Name).WriteCapacity)
				}
			}

			objectives = append(objectives, tmp)
		}
		return vars, constr, objectives, nil
	}

	simp := clp.NewSimplex()
	simp.SetOptimizationDirection(clp.Minimize)

	vars := make([]float64, 0)
	constr := make([][2]float64, 0)
	obj := make([][]float64, 0)

	if optimize == Load {
		vars, constr, obj = load(readFraction, vars, constr, loadLimit, frLoad)
	} else if optimize == Network {
		vars, constr, obj = network(nil)
	} else if optimize == Latency {
		vars, constr, obj, _ = latency(nil)
	}

	readQConstraint, writeQConstraint := defineBaseConstraints(optimize, readQuorumVars, writeQuorumVars)
	obj = append(obj, readQConstraint)
	obj = append(obj, writeQConstraint)

	if loadLimit != nil {
		_, _, lobj := load(readFraction, vars, constr, loadLimit, frLoad)
		vars = append(vars, 0)

		for r := 0; r < len(obj); r++ {
			if len(obj[r]) != len(vars) {
				obj[r] = insertAt(obj[r], len(obj[r])-1, 0.0)
			}
		}

		b := [2]float64{math.Inf(-1), math.Inf(1)}
		constr = append(constr, b)

		obj = append(obj, lobj...)
	}

	if networkLimit != nil {
		_, _, lobj := network(networkLimit)

		if len(obj[0]) != len(lobj[0]) {
			lobj[0] = insertAt(lobj[0], len(lobj[0])-1, 0)
		}
		obj = append(obj, lobj[0])
	}

	if latencyLimit != nil {
		_, _, lobj, _ := latency(latencyLimit)

		if len(obj[0]) != len(lobj[0]) {
			lobj[0] = insertAt(lobj[0], len(lobj[0])-1, 0)
		}

		obj = append(obj, lobj[0])
	}

	simp.EasyLoadDenseProblem(vars, constr, obj)
	// Solve the optimization problem.
	status := simp.Primal(clp.NoValuesPass, clp.NoStartFinishOptions)
	soln := simp.PrimalColumnSolution()

	if status != clp.Optimal {
		return nil, fmt.Errorf("no optimal strategy found")
	}

	fmt.Println(soln)

	readSigma := make([]SigmaRecord, 0)
	writeSigma := make([]SigmaRecord, 0)

	for _, v := range readQuorumVars {
		if soln[v.Index] != 0 {
			readSigma = append(readSigma, SigmaRecord{Quorum: *v.Quorum, Probability: soln[v.Index]})
		}
	}

	for _, v := range writeQuorumVars {
		if soln[v.Index] != 0 {
			writeSigma = append(writeSigma, SigmaRecord{Quorum: *v.Quorum, Probability: soln[v.Index]})
		}
	}

	newStrategy := DefStrategy(qs, Sigma{Values: readSigma}, Sigma{Values: writeSigma})

	return &newStrategy, nil
}

func defineOptimizationVars(readQuorums []ExprSet, writeQuorums []ExprSet) (readQuorumVars []lpVariable, xToReadQuorumVars map[GenericExpr][]lpVariable, writeQuorumVars []lpVariable, xToWriteQuorumVars map[GenericExpr][]lpVariable) {
	readQuorumVars = make([]lpVariable, 0)
	xToReadQuorumVars = make(map[GenericExpr][]lpVariable)

	for i, rq := range readQuorums {
		q := rq
		v := lpVariable{Name: fmt.Sprintf("r%b", i), UBound: 1, LBound: 0, Value: 1.0, Index: i, Quorum: &q}
		readQuorumVars = append(readQuorumVars, v)

		for n := range rq {

			if _, ok := xToReadQuorumVars[n]; !ok {
				xToReadQuorumVars[n] = []lpVariable{v}
				continue
			}
			xToReadQuorumVars[n] = append(xToReadQuorumVars[n], v)
		}
	}

	writeQuorumVars = make([]lpVariable, 0)
	xToWriteQuorumVars = make(map[GenericExpr][]lpVariable)

	for i, rq := range writeQuorums {
		q := rq
		v := lpVariable{Name: fmt.Sprintf("w%d", i), UBound: 1, LBound: 0, Value: 1.0, Index: len(readQuorums) + i, Quorum: &q}
		writeQuorumVars = append(writeQuorumVars, v)

		for n := range rq {
			if _, ok := xToWriteQuorumVars[n]; !ok {
				xToWriteQuorumVars[n] = []lpVariable{v}
				continue
			}
			xToWriteQuorumVars[n] = append(xToWriteQuorumVars[n], v)
		}
	}
	return readQuorumVars, xToReadQuorumVars, writeQuorumVars, xToWriteQuorumVars
}

func defineBaseConstraints(optimize OptimizeType, readQuorumVars []lpVariable, writeQuorumVars []lpVariable) (readQConstraint []float64, writeQConstraint []float64) {
	// read quorum constraint
	readQConstraint = make([]float64, 0)
	readQConstraint = append(readQConstraint, 1)

	for range readQuorumVars {
		readQConstraint = append(readQConstraint, 1.0)
	}

	for range writeQuorumVars {
		readQConstraint = append(readQConstraint, 0.0)
	}

	if optimize == Load {
		readQConstraint = append(readQConstraint, 0.0)
	}

	readQConstraint = append(readQConstraint, 1)

	// write quorum constraint
	writeQConstraint = make([]float64, 0)
	writeQConstraint = append(writeQConstraint, 1)

	for range readQuorumVars {
		writeQConstraint = append(writeQConstraint, 0.0)
	}

	for range writeQuorumVars {
		writeQConstraint = append(writeQConstraint, 1.0)
	}

	if optimize == Load {
		writeQConstraint = append(writeQConstraint, 0.0)
	}

	writeQConstraint = append(writeQConstraint, 1)
	return readQConstraint, writeQConstraint
}

func load(readFraction map[float64]float64, vars []float64, constr [][2]float64, loadLimit *float64,
	frLoad func(loadLimit *float64, fr float64) ([]float64, [][2]float64, [][]float64, error)) ([]float64, [][2]float64, [][]float64) {
	objTemp := make([][]float64, 0)
	objs := make([][]float64, 0)

	for fr, p := range readFraction {
		vars, constr, objTemp, _ = frLoad(nil, fr)

		for r := 0; r < len(objTemp); r++ {

			for c := 0; c < len(objTemp[r]); c++ {
				objTemp[r][c] = objTemp[r][c] * p
			}

			ninf := math.Inf(-1)

			loadLimitValue := 1.0

			if loadLimit != nil {
				loadLimitValue = *loadLimit
			}

			objTemp[r] = append([]float64{ninf}, objTemp[r]...)
			objTemp[r][len(objTemp[r])-1] = -loadLimitValue
			objTemp[r] = append(objTemp[r], 0)

			objs = append(objs, objTemp...)
		}

	}

	return vars, constr, objTemp
}

func insertAt(a []float64, index int, value float64) []float64 {
	if len(a) == index { // nil or empty slice or after last element
		return append(a, value)
	}
	a = append(a[:index+1], a[index:]...) // index < len(a)
	a[index] = value
	return a
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

func DefStrategy(quorumSystem QuorumSystem, sigmaR Sigma, sigmaW Sigma) Strategy {
	newStrategy := Strategy{SigmaR: sigmaR, SigmaW: sigmaW, Qs: quorumSystem}

	xReadProbability := make(map[Node]float64)
	for _, sr := range sigmaR.Values {
		for q := range sr.Quorum {
			xReadProbability[q.(Node)] += sr.Probability
		}

	}

	xWriteProbability := make(map[Node]float64)
	for _, sr := range sigmaW.Values {
		for q := range sr.Quorum {
			xWriteProbability[q.(Node)] += sr.Probability
		}
	}

	newStrategy.XWriteProbability = xWriteProbability
	newStrategy.XReadProbability = xReadProbability

	return newStrategy
}

func (s Strategy) String() string {
	return ""
}

func (s Strategy) GetReadQuorum() ExprSet {

	rand.Seed(time.Now().UTC().UnixNano()) // always seed random!

	criteria := make([]wr.Choice, 0)

	weightSum := 0.0
	for _, w := range s.SigmaR.Values {
		weightSum += w.Probability
	}

	for _, sigmaRecord := range s.SigmaR.Values {
		criteria = append(criteria, wr.Choice{Item: sigmaRecord.Quorum, Weight: uint(sigmaRecord.Probability * 10)})
	}

	chooser, _ := wr.NewChooser(criteria...)
	result := chooser.Pick().(ExprSet)

	return result
}

func (s Strategy) GetWriteQuorum() ExprSet {

	rand.Seed(time.Now().UTC().UnixNano()) // always seed random!

	criteria := make([]wr.Choice, 0)

	weightSum := 0.0
	for _, w := range s.SigmaW.Values {
		weightSum += w.Probability
	}

	for _, sigmaRecord := range s.SigmaW.Values {
		criteria = append(criteria, wr.Choice{Item: sigmaRecord.Quorum, Weight: uint(sigmaRecord.Probability * 10)})
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

	total := frsum*reads + (1-frsum)*writes
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
	return fr*s.XReadProbability[node]/float64(*node.ReadCapacity) +
		fw*s.XWriteProbability[node]/float64(*node.WriteCapacity)
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
