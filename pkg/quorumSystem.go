package pkg

import (
	"fmt"
	"github.com/lanl/clp"
	"math"
	"sort"
)

// nameToNode keeps track of the name to node mapping ( "a"-> Node("a")).
type nameToNode = map[string]Node

//lpVariable describe a linear programming variable for the quorum system.
type lpVariable struct {
	Name   string
	Index  int
	Value  float64
	UBound float64
	LBound float64
	Quorum ExprSet
}

// lpDefinition defines a linear programming expression with its own Vars, Constraints, Objectives.
//
// e.g:
// Let's suppose that we have 3 six-faced dices. Two dice are allowed have the same value.
// The objective is to find the difference in value between the 1st and 2nd-largest dice must be smaller
// than the difference in value between the 2nd and 3rd dice.
//
// This problem can be represented with the math equations:
//
//	Vars: diceA, diceB, diceC
//  Constraints:
// 		1 ≤ diceA ≤ 6
//		1 ≤ diceB ≤ 6
//		1 ≤ diceC ≤ 6
//		1 ≤ diceA - diceB ≤ ∞ | The two dices cannot be the same.
//		1 ≤ diceB - diceC ≤ ∞ | The two dices cannot be the same.
//  Obj:
//		a − b < b − c -> -∞ ≤ a - 2b + c ≤ -1 | The main objective codified from the problem.
//
// The problem can also be represented using an lpDefinition as follows:

type lpDefinition struct {
	Vars        []float64
	Constraints [][2]float64
	Objectives  [][]float64
}

// newDefinitionWithVars
func newDefinitionWithVarsAndConstraints(vars ...[]lpVariable) lpDefinition {
	def := lpDefinition{}
	def.Vars = make([]float64, 0)
	def.Constraints = make([][2]float64, 0)
	def.Objectives = make([][]float64, 0)

	for _, vArr := range vars {
		for _, v := range vArr {
			def.Vars = append(def.Vars, 1.0)
			b := [2]float64{v.LBound, v.UBound}
			def.Constraints = append(def.Constraints, b)

		}
	}

	return def
}

// QuorumSystem describes a read-write quorum system.
type QuorumSystem struct {
	// reads describes the read-quorum.
	reads Expr
	// writes describes the write-quorum.
	writes Expr
	// nameToNode keeps track the name of a node to a GetNodeByName.
	nameToNode nameToNode
}

// NewQuorumSystem defines a new quorum system given the reads Expr and the writes Expr.
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

// NewQuorumSystemWithReads defines a new quorum system given a read Expr, the write Expr is derived using DualOperator.Dual operation.
func NewQuorumSystemWithReads(reads Expr) QuorumSystem {
	qs, _ := NewQuorumSystem(reads, reads.Dual())

	qs.nameToNode = nameToNode{}

	for node := range qs.GetNodes() {
		qs.nameToNode[node.Name] = node
	}

	return qs
}

// NewQuorumSystemWithWrites defines a new quorum system given a write Expr, the read Expr is derived using DualOperator.Dual operation.
func NewQuorumSystemWithWrites(writes Expr) QuorumSystem {
	qs := QuorumSystem{reads: writes.Dual(), writes: writes}

	qs.nameToNode = nameToNode{}

	for node := range qs.GetNodes() {
		qs.nameToNode[node.Name] = node
	}

	return qs
}

// Capacity calculate and gets the capacity from the optimized Strategy.
func (qs QuorumSystem) Capacity(strategyOptions StrategyOptions) (float64, error) {

	strategy, err := qs.Strategy(initStrategyOptions(strategyOptions))

	if err != nil {
		return -1, err
	}

	return strategy.Capacity(&strategyOptions.ReadFraction, &strategyOptions.WriteFraction)
}

// Latency calculate and gets the latency from the optimized Strategy.
func (qs QuorumSystem) Latency(strategyOptions StrategyOptions) (float64, error) {

	strategy, err := qs.Strategy(initStrategyOptions(strategyOptions))

	if err != nil {
		return -1, err
	}

	return strategy.Latency(&strategyOptions.ReadFraction, &strategyOptions.WriteFraction)
}

// Load calculate and gets the Load from the optimized Strategy.
func (qs QuorumSystem) Load(strategyOptions StrategyOptions) (float64, error) {

	strategy, err := qs.Strategy(initStrategyOptions(strategyOptions))

	if err != nil {
		return -1, err
	}

	return strategy.Load(&strategyOptions.ReadFraction, &strategyOptions.WriteFraction)
}

// NetworkLoad calculate and gets the NetworkLoad from the optimized Strategy.
func (qs QuorumSystem) NetworkLoad(strategyOptions StrategyOptions) (float64, error) {

	strategy, err := qs.Strategy(initStrategyOptions(strategyOptions))

	if err != nil {
		return -1, err
	}

	return strategy.NetworkLoad(&strategyOptions.ReadFraction, &strategyOptions.WriteFraction)
}

//ReadQuorums gets the read quorums.
func (qs QuorumSystem) ReadQuorums() chan ExprSet {
	return qs.reads.Quorums()
}

//WriteQuorums gets the write quorums.
func (qs QuorumSystem) WriteQuorums() chan ExprSet {
	return qs.writes.Quorums()
}

// ListReadQuorums fetches the read quorums and returns as an []ExprSet.
func (qs QuorumSystem) ListReadQuorums() []ExprSet {
	rq := make([]ExprSet, 0)

	for e := range qs.ReadQuorums() {
		rq = append(rq, e)
	}

	return rq
}

// ListWriteQuorums fetches the write quorums and returns as an []ExprSet.
func (qs QuorumSystem) ListWriteQuorums() []ExprSet {
	wq := make([]ExprSet, 0)

	for e := range qs.WriteQuorums() {
		wq = append(wq, e)
	}

	return wq
}

// IsReadQuorum check if a set of expression is a read quorum.
func (qs QuorumSystem) IsReadQuorum(xs ExprSet) bool {
	return qs.reads.IsQuorum(xs)
}

// IsWriteQuorum check if a set of expression is a write quorum.
func (qs QuorumSystem) IsWriteQuorum(xs ExprSet) bool {
	return qs.writes.IsQuorum(xs)
}

// GetNodeByName returns a node by its name.
func (qs QuorumSystem) GetNodeByName(name string) Node {
	return qs.nameToNode[name]
}

//GetNodes returns a set of nodes.
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

// GetNodesAsArray returns an array of Node.
func (qs QuorumSystem) GetNodesAsArray() []Node {
	nodes := make([]Node, 0)
	for n := range qs.GetNodes() {
		nodes = append(nodes, n)
	}
	return nodes
}

// Resilience returns the total resilience of the quorum system - min(readResilience, writeResilience)
func (qs QuorumSystem) Resilience() uint {
	rres := qs.ReadResilience()
	wres := qs.WriteResilience()

	if rres < wres {
		return rres
	}

	return wres
}

// ReadResilience returns the read resilience.
func (qs QuorumSystem) ReadResilience() uint {
	return qs.reads.Resilience()
}

// WriteResilience returns the write resilience.
func (qs QuorumSystem) WriteResilience() uint {
	return qs.writes.Resilience()
}

// DupFree returns true if the quorum system is duplicate free, otherwise false.
func (qs QuorumSystem) DupFree() bool {
	return qs.reads.DupFree() && qs.writes.DupFree()
}

// Strategy returns the optimal Strategy for the given quorum system.
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
		return nil, fmt.Errorf("a getLoadObjective limit cannot be set when optimizing for getLoadObjective")
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

	d, err := canonicalizeReadsWrites(&sb.ReadFraction, &sb.WriteFraction)

	if err != nil {
		return nil, err
	}

	// no resilience target
	if sb.F == 0 {
		return qs.loadOptimalStrategy(sb.Optimize, rq, wq, d,
			sb.LoadLimit, sb.NetworkLimit, sb.LatencyLimit)
	}

	xs := qs.GetNodesAsArray()

	rq = make([]ExprSet, 0)
	wq = make([]ExprSet, 0)

	for _, e := range qs.getResilientQuorums(sb.F, xs, qs.reads) {
		rq = append(rq, e)
	}

	for _, e := range qs.getResilientQuorums(sb.F, xs, qs.writes) {
		wq = append(wq, e)
	}

	if len(rq) == 0 || len(wq) == 0 {
		return nil, fmt.Errorf("there are no %d-resilient read quorums", sb.F)
	}

	return qs.loadOptimalStrategy(sb.Optimize, rq, wq, d,
		sb.LoadLimit, sb.NetworkLimit, sb.LatencyLimit)
}

// UniformStrategy returns the standard majority quorum strategy for the quorum system.
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

// MakeStrategy returns a strategy given the read and write sigma.
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

// getResilientQuorums returns a set of quorums that has a resilience f.
func (qs QuorumSystem) getResilientQuorums(f int, n []Node, e Expr) []ExprSet {
	cur := ExprSet{}
	result := make([]ExprSet, 0)
	return getResilientQuorumsHelper(result, f, n, e, cur, 0)
}

func getResilientQuorumsHelper(exprSets []ExprSet, minResilience int, n []Node, e Quorum, cur ExprSet, i int) []ExprSet {
	minf := minResilience

	if minResilience > len(cur) {
		minf = len(cur)
	}

	isQuorum := true
	combinationSets := combinations(setToArr(cur), minf)

	for _, failure := range combinationSets {
		if !e.IsQuorum(remove(cur, failure...)) {
			isQuorum = false
		}
	}

	if isQuorum && len(combinationSets) > 0 {
		exprSets = append(exprSets, cur)
		return exprSets
	}

	for j := i; j < len(n); j++ {
		cur[n[j]] = true
		defer delete(cur, n[j])
		return getResilientQuorumsHelper(exprSets, minResilience, n, e, copySet(cur), j+1)
	}
	return exprSets
}

// readQuorumLatency return the latency of a read quorum.
func (qs QuorumSystem) readQuorumLatency(quorum []Node) (uint, error) {
	return qs.quorumLatency(quorum, qs.IsReadQuorum)
}

// writeQuorumLatency returns the latency of a write quorum.
func (qs QuorumSystem) writeQuorumLatency(quorum []Node) (uint, error) {
	return qs.quorumLatency(quorum, qs.IsWriteQuorum)
}

// quorumLatency returns the maximum latency of a given quorum.
func (qs QuorumSystem) quorumLatency(quorum []Node, isQuorum func(set ExprSet) bool) (uint, error) {
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
			return *sortedQ[i].Latency, nil
		}
	}

	return 0, fmt.Errorf("_quorum_latency called on a non-quorum")

}

func (qs QuorumSystem) loadOptimalStrategy(
	optimize OptimizeType,
	readQuorums []ExprSet,
	writeQuorums []ExprSet,
	readFraction DistributionValues,
	loadLimit *float64,
	networkLimit *float64,
	latencyLimit *float64) (*Strategy, error) {

	ninf := math.Inf(-1)
	pinf := math.Inf(1)

	readQuorumVars, xToReadQuorumVars := getOptimizationVars(readQuorums, "r%d", 0)
	writeQuorumVars, xToWriteQuorumVars := getOptimizationVars(writeQuorums, "w%d", len(readQuorums))

	fr := 0.0

	for k, v := range readFraction {
		fr += k * v
	}

	network := func(networkLimit *float64) lpDefinition {
		def := newDefinitionWithVarsAndConstraints(readQuorumVars, writeQuorumVars)

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
		def := newDefinitionWithVarsAndConstraints(readQuorumVars, writeQuorumVars)

		// building latency objs | -inf <= latency_def <= inf
		objExpr := make([]float64, len(def.Vars))

		for _, v := range readQuorumVars {
			nodes := make([]Node, 0)

			for x := range v.Quorum {
				q := qs.GetNodeByName(x.String())
				nodes = append(nodes, q)
			}

			l, err := qs.readQuorumLatency(nodes)

			if err != nil {
				return lpDefinition{}, fmt.Errorf("error on readQuorumLatency %s", err)
			}

			objExpr[v.Index] = fr * v.Value * float64(l)
		}

		for _, v := range writeQuorumVars {
			nodes := make([]Node, 0)

			for x := range v.Quorum {
				q := qs.GetNodeByName(x.String())
				nodes = append(nodes, q)
			}

			l, err := qs.writeQuorumLatency(nodes)

			if err != nil {
				return lpDefinition{}, fmt.Errorf("error on writeQuorumLatency %s", err)
			}

			objExpr[v.Index] = (1 - fr) * v.Value * float64(l)
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
		def := newDefinitionWithVarsAndConstraints(readQuorumVars, writeQuorumVars)
		// l def
		def.Vars = append(def.Vars, 1.0)
		def.Constraints = append(def.Constraints, [2]float64{ninf, pinf})

		// Load formula
		for n := range qs.GetNodes() {
			tmp := make([]float64, len(def.Vars))

			if _, ok := xToReadQuorumVars[n]; ok {
				vs := xToReadQuorumVars[n]
				for _, v := range vs {
					tmp[v.Index] += fr * v.Value / float64(*qs.GetNodeByName(n.Name).ReadCapacity)
				}
			}

			if _, ok := xToWriteQuorumVars[n]; ok {
				vs := xToWriteQuorumVars[n]
				for _, v := range vs {
					tmp[v.Index] += (1 - fr) * v.Value / float64(*qs.GetNodeByName(n.Name).WriteCapacity)
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
		def = getLoadObjective(readFraction, loadLimit, frLoad)
	} else if optimize == Network {
		def = network(nil)
	} else if optimize == Latency {
		def, _ = latency(nil)
	}

	readQConstraint, writeQConstraint := getBaseConstraints(optimize, readQuorumVars, writeQuorumVars)
	def.Objectives = append(def.Objectives, readQConstraint)
	def.Objectives = append(def.Objectives, writeQConstraint)

	if loadLimit != nil {
		defTemp := getLoadObjective(readFraction, loadLimit, frLoad)
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
		def.Objectives = merge(def.Objectives, defTemp.Objectives)
	}

	if latencyLimit != nil {
		defTemp, _ := latency(latencyLimit)
		def.Objectives = merge(def.Objectives, defTemp.Objectives)
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

// getOptimizationVars returns the list lpVariable for a list of quorums.
func getOptimizationVars(quorums []ExprSet, name string, startIndex int) (quorumVars []lpVariable, quorumToQuorumVar map[Expr][]lpVariable) {
	quorumVars = make([]lpVariable, 0)
	quorumToQuorumVar = make(map[Expr][]lpVariable)

	for i, rq := range quorums {
		q := rq
		v := lpVariable{Name: fmt.Sprintf(name, i), UBound: 1, LBound: 0, Value: 1.0, Index: i + startIndex, Quorum: q}
		quorumVars = append(quorumVars, v)

		for n := range rq {

			if _, ok := quorumToQuorumVar[n]; !ok {
				quorumToQuorumVar[n] = []lpVariable{v}
				continue
			}
			quorumToQuorumVar[n] = append(quorumToQuorumVar[n], v)
		}
	}

	return quorumVars, quorumToQuorumVar
}

// getBaseConstraints returns the list of lpVariable for the strategy optimization process.
func getBaseConstraints(optimize OptimizeType, readQuorumVars []lpVariable, writeQuorumVars []lpVariable) (readQConstraint []float64, writeQConstraint []float64) {
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

// getLoadObjective returns the lpDefinition for the load objective.
func getLoadObjective(readFraction DistributionValues, loadLimit *float64,
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

func merge(obj [][]float64, lobj [][]float64) [][]float64 {

	if len(obj[0]) != len(lobj[0]) {
		lobj[0] = insertAt(lobj[0], len(lobj[0])-1, 0)
	}
	obj = append(obj, lobj[0])

	return obj
}

func remove(set ExprSet, g ...Expr) ExprSet {
	newSet := copySet(set)

	for _, e := range g {
		delete(newSet, e)
	}

	return newSet
}

func copySet(set ExprSet) ExprSet {
	newSet := make(ExprSet)

	for k, v := range set {
		newSet[k] = v
	}
	return newSet
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
