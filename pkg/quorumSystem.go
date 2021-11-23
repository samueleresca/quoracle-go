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


type nameToNode = map[string]Node

type OptimizeType string

const (
	Load    OptimizeType = "Load"
	Network OptimizeType = "Network"
	Latency OptimizeType = "Latency"
)

type QuorumSystem struct {
	reads   Expr
	writes     Expr
	nameToNode nameToNode
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
	Quorum ExprSet
}

type lpDefinition struct {
	Vars        []float64
	Constraints [][2]float64
	Objectives  [][]float64
}

func NewQuorumSystem(reads Expr, writes Expr) (QuorumSystem, error) {
	optionalWrites := reads.Dual()

	for k := range writes.Quorums() {
		if !optionalWrites.IsQuorum(k) {
			return QuorumSystem{}, fmt.Errorf("not all read quorums intersect all write quorums")
		}
	}
	qs := QuorumSystem{reads: reads, writes: writes}

	qs.nameToNode = nameToNode{}

	for node := range qs.GetNodes() {
		qs.nameToNode[node.Name] = node
	}

	return qs, nil
}

func NewQuorumSystemWithReads(reads Expr) QuorumSystem {
	qs, _ := NewQuorumSystem(reads, reads.Dual())

	qs.nameToNode = nameToNode{}

	for node := range qs.GetNodes() {
		qs.nameToNode[node.Name] = node
	}

	return qs
}

func NewQuorumSystemWithWrites(writes Expr) QuorumSystem {
	qs := QuorumSystem{reads: writes.Dual(), writes: writes}

	qs.nameToNode = nameToNode{}

	for node := range qs.GetNodes() {
		qs.nameToNode[node.Name] = node
	}

	return qs
}

func (qs QuorumSystem) String() string {
	return "TODO"
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
	return qs.reads.Quorums()
}

func (qs QuorumSystem) WriteQuorums() chan ExprSet {
	return qs.writes.Quorums()
}

func (qs QuorumSystem) ListReadQuorums() []ExprSet {
	rq := make([]ExprSet, 0)

	for e := range qs.ReadQuorums() {
		rq = append(rq, e)
	}

	return rq
}

func (qs QuorumSystem) ListWriteQuorums() []ExprSet {
	wq := make([]ExprSet, 0)

	for e := range qs.WriteQuorums() {
		wq = append(wq, e)
	}

	return wq
}

func (qs QuorumSystem) IsReadQuorum(xs ExprSet) bool {
	return qs.reads.IsQuorum(xs)
}

func (qs QuorumSystem) IsWriteQuorum(xs ExprSet) bool {
	return qs.writes.IsQuorum(xs)
}

func (qs QuorumSystem) Node(x string) Node {
	return qs.nameToNode[x]
}

func (qs QuorumSystem) GetNodes() NodeSet {
	r := make(NodeSet, 0)

	for n := range qs.reads.GetNodes() {
		r[n] = true
	}

	for n := range qs.writes.GetNodes() {
		r[n] = true
	}

	return r
}

func (qs QuorumSystem) Elements() []Node {
	nodes := make([]Node, 0)
	for n := range qs.GetNodes() {
		nodes = append(nodes, n)
	}
	return nodes
}

func (qs QuorumSystem) Resilience() uint {
	rr := qs.ReadResilience()
	wr := qs.WriteResilience()

	if rr < wr {
		return rr
	}

	return wr
}

func (qs QuorumSystem) ReadResilience() uint {
	return qs.reads.Resilience()
}

func (qs QuorumSystem) WriteResilience() uint {
	return qs.writes.Resilience()
}

func (qs QuorumSystem) DupFree() bool {
	return qs.reads.DupFree() && qs.writes.DupFree()
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

	rq := qs.ListReadQuorums()
	wq := qs.ListWriteQuorums()

	d, err := canonicalizeRW(&sb.ReadFraction, &sb.WriteFraction)

	if err != nil {
		return nil, err
	}

	// no resilience target
	if sb.F == 0 {
		return qs.loadOptimalStrategy(sb.Optimize, rq, wq, d,
			sb.LoadLimit, sb.NetworkLimit, sb.LatencyLimit)
	}

	xs := qs.Elements()

	rq = make([]ExprSet, 0)
	wq = make([]ExprSet, 0)

	for _, e := range qs.fResilientQuorums(sb.F, xs, qs.reads) {
		rq = append(rq, e)
	}

	for _, e := range qs.fResilientQuorums(sb.F, xs, qs.writes) {
		wq = append(wq, e)
	}

	if len(rq) == 0 || len(wq) == 0 {
		return nil, fmt.Errorf("there are no %d-resilient read quorums", sb.F)
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
		readQuorums = qs.ListReadQuorums()
		writeQuorums = qs.ListWriteQuorums()
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

	return NewStrategy(qs, Sigma{sigmaR}, Sigma{sigmaW}), nil
}

func (qs QuorumSystem) MakeStrategy(sigmaR Sigma, sigmaW Sigma) (Strategy, error) {
	normalizedSigmaR := make([]SigmaRecord, 0)
	normalizedSigmaW := make([]SigmaRecord, 0)

	all := func(records []SigmaRecord, checkCondition func(record SigmaRecord) bool) bool {
		for _, r := range records {
			if !checkCondition(r) {
				return false
			}
		}
		return true
	}

	if !all(sigmaR.Values, func(r SigmaRecord) bool { return r.Probability >= 0 }) {
		return Strategy{}, fmt.Errorf("SigmaR has negative weights")
	}

	if !all(sigmaW.Values, func(r SigmaRecord) bool { return r.Probability >= 0 }) {
		return Strategy{}, fmt.Errorf("SigmaW has negative weights")
	}

	if !all(sigmaR.Values, func(r SigmaRecord) bool { return qs.IsReadQuorum(r.Quorum) }) {
		return Strategy{}, fmt.Errorf("SigmaR has non-read quorums")
	}

	if !all(sigmaW.Values, func(w SigmaRecord) bool { return qs.IsWriteQuorum(w.Quorum) }) {
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

	return NewStrategy(qs, Sigma{Values: normalizedSigmaR}, Sigma{Values: normalizedSigmaW}), nil
}

func (qs QuorumSystem) minimize(sets []ExprSet) []ExprSet {

	sort.Slice(sets, func(i, j int) bool {
		return len(sets[i]) < len(sets[j])
	})

	isSuperSet := func(x ExprSet, e ExprSet) bool {
		set := make(map[Expr]int)
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

func (qs QuorumSystem) fResilientQuorums(f int, xs []Node, e Expr) []ExprSet {
	s := ExprSet{}
	result := make([]ExprSet, 0)
	return fResilientHelper(result, f, xs, e, s, 0)
}

func fResilientHelper(result []ExprSet, f int, xs []Node, e Quorum, s ExprSet, i int) []ExprSet {
	minf := f

	if f > len(s) {
		minf = len(s)
	}

	isAll := true
	combinationSets := combinations(exprSetToArr(s), minf)

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

func removeFromExprSet(set ExprSet, g []Expr) ExprSet {
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

	ninf := math.Inf(-1)
	pinf := math.Inf(1)

	readQuorumVars, xToReadQuorumVars, writeQuorumVars, xToWriteQuorumVars := defineOptimizationVars(readQuorums, writeQuorums)

	fr := 0.0

	for k, v := range readFraction {
		fr += k * v
	}

	network := func(networkLimit *float64) lpDefinition {
		def := lpDefinition{}
		def.Vars = make([]float64, 0)
		def.Constraints = make([][2]float64, 0)
		def.Objectives = make([][]float64, 0)

		// initializes target array
		for _, v := range readQuorumVars {
			def.Vars = append(def.Vars, 1.0)
			b := [2]float64{v.LBound, v.UBound}
			def.Constraints = append(def.Constraints, b)

		}
		// add constraints 0 <= q <= 1
		for _, v := range writeQuorumVars {
			def.Vars = append(def.Vars, 1.0)
			b := [2]float64{v.LBound, v.UBound}
			def.Constraints = append(def.Constraints, b)
		}

		objExpr := make([]float64, len(def.Vars))

		// network_def  - inf <= network_def <= +inf
		for _, v := range readQuorumVars {
			objExpr[v.Index] = fr * float64(len(v.Quorum))
		}

		for _, v := range writeQuorumVars {
			objExpr[v.Index] = (1 - fr) * float64(len(v.Quorum))
		}

		objExpr = append([]float64{ninf}, objExpr...)

		if networkLimit == nil {
			objExpr = append(objExpr, pinf)
		} else {
			objExpr = append(objExpr, *networkLimit)
		}

		def.Objectives = append(def.Objectives, objExpr)

		return def
	}

	latency := func(latencyLimit *float64) (lpDefinition, error) {
		def := lpDefinition{}
		def.Vars = make([]float64, 0)
		def.Constraints = make([][2]float64, 0)
		def.Objectives = make([][]float64, 0)

		// initializes vars array
		for range readQuorumVars {
			def.Vars = append(def.Vars, 1.0)
		}

		for range writeQuorumVars {
			def.Vars = append(def.Vars, 1.0)
		}

		// add constraints 0 <= q <= 1
		for _, v := range readQuorumVars {
			b := [2]float64{v.LBound, v.UBound}
			def.Constraints = append(def.Constraints, b)
		}

		for _, v := range writeQuorumVars {
			b := [2]float64{v.LBound, v.UBound}
			def.Constraints = append(def.Constraints, b)
		}

		// building latency objs | -inf <= latency_def <= inf
		objExpr := make([]float64, len(def.Vars))

		for _, v := range readQuorumVars {
			quorum := make([]Node, 0)

			for x := range v.Quorum {
				q := qs.Node(x.String())
				quorum = append(quorum, q)
			}

			l, err := qs.readQuorumLatency(quorum)

			if err != nil {
				return lpDefinition{}, fmt.Errorf("error on readQuorumLatency %s", err)
			}

			objExpr[v.Index] = fr * v.Value * float64(*l)
		}

		for _, v := range writeQuorumVars {
			quorum := make([]Node, 0)

			for x := range v.Quorum {
				q := qs.Node(x.String())
				quorum = append(quorum, q)
			}

			l, err := qs.writeQuorumLatency(quorum)

			if err != nil {
				return lpDefinition{}, fmt.Errorf("Error on writeQuorumLatency %s", err)
			}

			objExpr[v.Index] = (1 - fr) * v.Value * float64(*l)
		}

		objExpr = append([]float64{ninf}, objExpr...)

		if latencyLimit == nil {
			objExpr = append(objExpr, pinf)
		} else {
			objExpr = append(objExpr, *latencyLimit)
		}
		def.Objectives = append(def.Objectives, objExpr)

		return def, nil
	}

	frLoad := func(loadLimit *float64, fr float64) (lpDefinition, error) {
		def := lpDefinition{}
		def.Vars = make([]float64, 0)
		def.Constraints = make([][2]float64, 0)
		def.Objectives = make([][]float64, 0)

		// initializes target array
		for _, v := range readQuorumVars {
			def.Vars = append(def.Vars, 1.0)
			b := [2]float64{v.LBound, v.UBound}
			def.Constraints = append(def.Constraints, b)

		}
		// add constraints 0 <= q <= 1
		for _, v := range writeQuorumVars {
			def.Vars = append(def.Vars, 1.0)
			b := [2]float64{v.LBound, v.UBound}
			def.Constraints = append(def.Constraints, b)
		}

		// l def
		def.Vars = append(def.Vars, 1.0)
		b := [2]float64{ninf, pinf}
		def.Constraints = append(def.Constraints, b)

		// Load formula

		for n := range qs.GetNodes() {
			tmp := make([]float64, len(def.Vars))

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

			def.Objectives = append(def.Objectives, tmp)
		}
		return def, nil
	}

	simp := clp.NewSimplex()
	simp.SetOptimizationDirection(clp.Minimize)

	def := lpDefinition{}
	def.Vars = make([]float64, 0)
	def.Constraints = make([][2]float64, 0)
	def.Objectives = make([][]float64, 0)

	if optimize == Load {
		def = load(readFraction, loadLimit, frLoad)
	} else if optimize == Network {
		def = network(nil)
	} else if optimize == Latency {
		def, _ = latency(nil)
	}

	readQConstraint, writeQConstraint := defineBaseConstraints(optimize, readQuorumVars, writeQuorumVars)
	def.Objectives = append(def.Objectives, readQConstraint)
	def.Objectives = append(def.Objectives, writeQConstraint)

	if loadLimit != nil {
		defTemp := load(readFraction, loadLimit, frLoad)
		def.Vars = append(def.Vars, 0)

		for r := 0; r < len(def.Objectives); r++ {
			if len(def.Objectives[r]) != len(def.Vars) {
				def.Objectives[r] = insertAt(def.Objectives[r], len(def.Objectives[r])-1, 0.0)
			}
		}

		b := [2]float64{ninf, pinf}
		def.Constraints = append(def.Constraints, b)
		def.Objectives = append(def.Objectives, defTemp.Objectives...)
	}

	if networkLimit != nil {
		defTemp := network(networkLimit)
		def.Objectives = appendObj(def.Objectives, defTemp.Objectives)
	}

	if latencyLimit != nil {
		defTemp, _ := latency(latencyLimit)
		def.Objectives = appendObj(def.Objectives, defTemp.Objectives)
	}

	simp.EasyLoadDenseProblem(def.Vars, def.Constraints, def.Objectives)
	// Solve the optimization problem.
	status := simp.Primal(clp.NoValuesPass, clp.NoStartFinishOptions)
	soln := simp.PrimalColumnSolution()

	if status != clp.Optimal {
		return nil, fmt.Errorf("no optimal strategy found")
	}

	readSigma := make([]SigmaRecord, 0)
	writeSigma := make([]SigmaRecord, 0)

	for _, v := range readQuorumVars {
		if soln[v.Index] != 0 {
			readSigma = append(readSigma, SigmaRecord{Quorum: v.Quorum, Probability: soln[v.Index]})
		}
	}

	for _, v := range writeQuorumVars {
		if soln[v.Index] != 0 {
			writeSigma = append(writeSigma, SigmaRecord{Quorum: v.Quorum, Probability: soln[v.Index]})
		}
	}

	newStrategy := NewStrategy(qs, Sigma{Values: readSigma}, Sigma{Values: writeSigma})

	return &newStrategy, nil
}

func appendObj(obj [][]float64, lobj [][]float64) [][]float64 {

	if len(obj[0]) != len(lobj[0]) {
		lobj[0] = insertAt(lobj[0], len(lobj[0])-1, 0)
	}
	obj = append(obj, lobj[0])

	return obj
}

func defineOptimizationVars(readQuorums []ExprSet, writeQuorums []ExprSet) (readQuorumVars []lpVariable, xToReadQuorumVars map[Expr][]lpVariable, writeQuorumVars []lpVariable, xToWriteQuorumVars map[Expr][]lpVariable) {
	readQuorumVars = make([]lpVariable, 0)
	xToReadQuorumVars = make(map[Expr][]lpVariable)

	for i, rq := range readQuorums {
		q := rq
		v := lpVariable{Name: fmt.Sprintf("r%b", i), UBound: 1, LBound: 0, Value: 1.0, Index: i, Quorum: q}
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
	xToWriteQuorumVars = make(map[Expr][]lpVariable)

	for i, rq := range writeQuorums {
		q := rq
		v := lpVariable{Name: fmt.Sprintf("w%d", i), UBound: 1, LBound: 0, Value: 1.0, Index: len(readQuorums) + i, Quorum: q}
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

func load(readFraction map[float64]float64, loadLimit *float64,
	frLoad func(loadLimit *float64, fr float64) (lpDefinition, error)) lpDefinition {

	def := lpDefinition{}
	ninf := math.Inf(-1)

	for fr, p := range readFraction {
		def, _ = frLoad(nil, fr)

		for r := 0; r < len(def.Objectives); r++ {

			for c := 0; c < len(def.Objectives[r]); c++ {
				def.Objectives[r][c] = def.Objectives[r][c] * p
			}
			loadLimitValue := 1.0

			if loadLimit != nil {
				loadLimitValue = *loadLimit
			}

			def.Objectives[r] = append([]float64{ninf}, def.Objectives[r]...)
			def.Objectives[r][len(def.Objectives[r])-1] = -loadLimitValue
			def.Objectives[r] = append(def.Objectives[r], 0)
		}
	}

	return def
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

func NewStrategy(quorumSystem QuorumSystem, sigmaR Sigma, sigmaW Sigma) Strategy {
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
	return "TODO"
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
		sum += p * s.maxLoad(fr)
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
		sum += p * 1.0 / s.maxLoad(fr)
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

		v, err := s.Qs.readQuorumLatency(nodes)

		if err != nil {
			return nil, err
		}

		reads += float64(*v) * rq.Probability
	}

	writes := 0.0

	for _, wq := range s.SigmaW.Values {
		nodes := make([]Node, 0)

		for n := range wq.Quorum {
			nodes = append(nodes, s.Qs.Node(n.String()))
		}

		v, err := s.Qs.writeQuorumLatency(nodes)

		if err != nil {
			return nil, err
		}
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

func (s Strategy) maxLoad(fr float64) float64 {
	max := 0.0

	for n := range s.Qs.GetNodes() {
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
	return s.nodeLoad(node, fr) / s.maxLoad(fr)
}

func (s Strategy) nodeThroughput(node Node, fr float64) float64 {
	capacity := 1 / s.maxLoad(fr)
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
