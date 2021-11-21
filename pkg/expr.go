package pkg

import (
	"fmt"
	"github.com/lanl/clp"
	"math"
	"math/bits"
	"reflect"
	"sort"
	"strings"
)

// ExprSet describes a set of Expr.
type ExprSet = map[Expr]bool
// NodeSet describes a set of Node.
type NodeSet = map[Node]bool

// ExprOperator that wraps the Add and Multiply methods needed to build a quorum from a set of Node.
type ExprOperator interface {
	// Add method aggregate a Node to an Expr with a logical Or (a ∨ b)
	// returns the resulting Or operation.
	Add(expr Expr) Or
	// Multiply method aggregate a Node to an Expr with a logical And (a ∧ b)
	// returns the resulting And operation.
	Multiply(expr Expr) And
}

// ExprGetter wraps some methods to retrieve the Expr.
type ExprGetter interface {
	// GetExprs methods returns a []Expr representing the Expr.
	GetExprs() []Expr
}
// NodeGetter wraps the method for getting the NodeSet from an Expr.
type NodeGetter interface {
	// GetNodes returns a NodeSet with the NodeSet in an Expr.
	GetNodes() NodeSet
}
// NumLeavesGetter wraps the method for getting the number of leaves in an Expr.
type NumLeavesGetter interface {
	// NumLeaves returns the number of leaves in an Expr. e.g. ( a + b ) * a results in 3 leaves.
	NumLeaves() uint
}

// DualOperator wraps a basic Dual method.
type DualOperator interface {
	// Dual method returns the logic Dual of an Expr. The Dual of a boolean Expr is the Expr one obtains
	// by interchanging addition and multiplication and interchanging 0’s and 1’s.
	// see: https://www.cs.fsu.edu/~lacher/courses/MAD3105/lectures/s4_1boolfn.pdf
	Dual() Expr
}

// ResilienceCalculator wraps the method for calculating the resilience of a quorum.
type ResilienceCalculator interface {
	// Resilience returns the resilience of an Expr.
	Resilience() uint
}

// MinFailuresCalculator wraps the method for calculating the minimum failure.
type MinFailuresCalculator interface {
	// MinFailures returns the number of minimum failures for an Expr.
	MinFailures() uint
}

// DuplicateChecker wraps the method for checking if an Expr contains a duplicate.
type DuplicateChecker interface {
	DupFree() bool
}

// Quorum wraps the methods for calculating a quorum from an Expr and to check if an ExprSet is a valid Quorum.
type Quorum interface {
	// Quorums returns a chan exposing the quorums derived from an Expr.
	Quorums() chan ExprSet
	// IsQuorum returns true if the ExprSet is a quorum otherwise it returns false.
	IsQuorum(set ExprSet) bool
}

// Expr represent a logic expressions between nodes or other expressions and its own methods.
type Expr interface {
	Quorum
	ExprOperator
	DualOperator
	ExprGetter
	NodeGetter
	NumLeavesGetter
	DuplicateChecker
	MinFailuresCalculator
	ResilienceCalculator
	fmt.Stringer
}

// Node represents a node in an Expr.
type Node struct {
	Name          string
	ReadCapacity  *uint
	WriteCapacity *uint
	Latency       *uint
}

// NewNode define a new node with a name.
func NewNode(name string) Node {
	node := Node{}
	node.Name = name

	initialValue := uint(1)
	node.ReadCapacity = &initialValue
	node.WriteCapacity = &initialValue

	return node
}

// NewNodeWithCapacityAndLatency defines a new node with a name, read and write capacities and a latency.
func NewNodeWithCapacityAndLatency(name string, readCapacity uint, writeCapacity uint, latency uint) Node {
	node := Node{}

	node.Name = name
	node.ReadCapacity = &readCapacity
	node.WriteCapacity = &writeCapacity
	node.Latency = &latency

	return node
}

// NewNodeWithCapacity defines a new node with a name a read and write capacity.
func NewNodeWithCapacity(name string, readCapacity uint, writeCapacity uint) Node {
	node := Node{}

	node.Name = name
	node.ReadCapacity = &readCapacity
	node.WriteCapacity = &writeCapacity

	return node
}

// NewNodeWithLatency defines a new node with a name and a latency.
func NewNodeWithLatency(name string, latency uint) Node {
	node := Node{}

	node.Name = name
	initialValue := uint(1)
	node.ReadCapacity = &initialValue
	node.WriteCapacity = &initialValue
	node.Latency = &latency

	return node
}

func (n Node) Add(expr Expr) Or {
	return mergeWithOr(n, expr)
}

func (n Node) Multiply(expr Expr) And {
	return mergeWithAnd(n, expr)
}

func (n Node) Quorums() chan ExprSet {
	chnl := make(chan ExprSet)

	go func() {
		chnl <- ExprSet{n: true}
		close(chnl)
	}()

	return chnl
}

func (n Node) IsQuorum(xs ExprSet) bool {
	for k := range xs {
		if n.String() == k.String() {
			return true
		}
	}

	return false
}

func (n Node) GetNodes() NodeSet {
	return NodeSet{n: true}
}

func (n Node) NumLeaves() uint {
	return 1
}

func (n Node) MinFailures() uint {
	return 1
}

func (n Node) Resilience() uint {
	if n.DupFree() {
		return n.MinFailures() - 1
	}

	qs := make([]ExprSet, 0)

	for q := range n.Quorums() {
		qs = append(qs, q)
	}

	return minHittingSet(qs) - 1.0
}

func (n Node) DupFree() bool {
	return uint(len(n.GetNodes())) == n.NumLeaves()
}

func (n Node) String() string {
	return n.Name
}

func (n Node) GetType() string {
	return "Node"
}

func (n Node) GetExprs() []Expr {
	return []Expr{n}
}

func (n Node) Dual() Expr {
	return n
}

// Or represents a logical Or expression between others nodes or expressions.
type Or struct {
	Es []Expr
}

func (e Or) Add(rhs Expr) Or {
	return mergeWithOr(e, rhs)
}

func (e Or) Multiply(rhs Expr) And {
	return mergeWithAnd(e, rhs)
}

func (e Or) Quorums() chan ExprSet {
	chnl := make(chan ExprSet)
	go func() {
		for _, es := range e.Es {
			tmp := <-es.Quorums()
			chnl <- tmp

		}
		// Ensure that at the end of the loop we close the channel!
		close(chnl)
	}()
	return chnl
}

func (e Or) IsQuorum(xs ExprSet) bool {
	var found = false
	for _, es := range e.Es {
		if es.IsQuorum(xs) {
			found = true
			return found
		}
	}
	return found
}

func (e Or) GetNodes() NodeSet {
	var final = make(NodeSet)

	for _, es := range e.Es {
		for n := range es.GetNodes() {
			final[n] = true
		}
	}
	return final
}

func (e Or) NumLeaves() uint {
	total := uint(0)

	for _, es := range e.Es {
		total += es.NumLeaves()
	}

	return total
}

func (e Or) MinFailures() uint {
	total := uint(0)

	for _, es := range e.Es {
		total += es.MinFailures()
	}

	return total
}

func (e Or) Resilience() uint {
	if e.DupFree() {
		return e.MinFailures() - 1
	}

	qs := make([]ExprSet, 0)

	for q := range e.Quorums() {
		qs = append(qs, q)
	}

	return minHittingSet(qs) - 1.0
}

func (e Or) DupFree() bool {
	return uint(len(e.GetNodes())) == e.NumLeaves()
}

func (e Or) String() string {

	if len(e.Es) == 0 {
		return "()"
	}
	var sb strings.Builder

	sb.WriteString("(")
	sb.WriteString(e.Es[0].String())

	for _, v := range e.Es[1:] {
		sb.WriteString(" + ")
		sb.WriteString(v.String())
	}

	sb.WriteString(")")
	return sb.String()
}

func (e Or) GetType() string {
	return "Or"
}

func (e Or) GetExprs() []Expr {
	return e.Es
}

func (e Or) Dual() Expr {
	dualExprs := make([]Expr, 0)
	for _, es := range e.Es {
		dualExprs = append(dualExprs, es.Dual())
	}
	return And{Es: dualExprs}
}

//And represents a logical And expression between others nodes or expressions.
type And struct {
	Es []Expr
}

func (e And) Add(rhs Expr) Or {
	return mergeWithOr(e, rhs)
}

func (e And) Multiply(rhs Expr) And {
	return mergeWithAnd(e, rhs)
}

func (e And) Quorums() chan ExprSet {
	chnl := make(chan ExprSet)
	flatQuorums := make([][]interface{}, 0)

	for _, es := range e.Es {
		quorums := make([]interface{}, 0)

		for q := range es.Quorums() {
			quorums = append(quorums, q)
		}
		flatQuorums = append(flatQuorums, quorums)
	}

	go func() {
		for _, sets := range product(flatQuorums...) {
			set := make(ExprSet)
			for _, t := range sets {
				set = mergeExprSets(set, t.(ExprSet))
			}
			chnl <- set
		}

		// Ensure that at the end of the loop we close the channel!
		close(chnl)
	}()
	return chnl
}

func (e And) IsQuorum(xs ExprSet) bool {
	var found = true
	for _, es := range e.Es {
		if !es.IsQuorum(xs) {
			found = false
			return found
		}
	}
	return found
}

func (e And) GetNodes() NodeSet {
	var final = make(NodeSet)

	for _, es := range e.Es {
		for n := range es.GetNodes() {
			final[n] = true
		}
	}
	return final
}

func (e And) NumLeaves() uint {
	total := uint(0)

	for _, es := range e.Es {
		total += es.NumLeaves()
	}

	return total
}

func (e And) MinFailures() uint {
	var exprs = e.Es
	var min = exprs[0].MinFailures()

	for _, expr := range exprs {
		if min > expr.MinFailures() {
			min = expr.MinFailures()
		}
	}
	return min
}

func (e And) Resilience() uint {
	if e.DupFree() {
		return e.MinFailures() - 1
	}

	qs := make([]ExprSet, 0)

	for q := range e.Quorums() {
		qs = append(qs, q)
	}

	return minHittingSet(qs) - 1
}

func (e And) DupFree() bool {
	return uint(len(e.GetNodes())) == e.NumLeaves()
}

func (e And) String() string {
	if len(e.Es) == 0 {
		return "()"
	}
	var sb strings.Builder

	sb.WriteString("(")
	sb.WriteString(e.Es[0].String())

	for _, v := range e.Es[1:] {
		sb.WriteString(" * ")
		sb.WriteString(v.String())
	}

	sb.WriteString(")")

	return sb.String()
}

func (e And) GetType() string {
	return "And"
}

func (e And) GetExprs() []Expr {
	return e.Es
}

func (e And) Dual() Expr {
	dualExprs := make([]Expr, 0)
	for _, es := range e.Es {
		dualExprs = append(dualExprs, es.Dual())
	}
	return Or{Es: dualExprs}
}

// Choose represents a logical
type Choose struct {
	Es []Expr
	K  int
}

func NewChoose(k int, es []Expr) (Expr, error) {
	if len(es) == 0 {
		return Choose{}, fmt.Errorf("no expressions provided")
	}

	if !(1 <= k && k <= len(es)) {
		return Choose{}, fmt.Errorf("k must be in the range [1, len(es)]")
	}

	if k == 1 {
		return Or{Es: es}, nil
	}

	if k == len(es) {
		return And{Es: es}, nil
	}

	if k <= 0 || k > len(es) {
		return Choose{}, fmt.Errorf("k must be in the range [1, %d]", len(es))
	}

	return Choose{Es: es, K: k}, nil
}

func (e Choose) Add(rhs Expr) Or {
	return mergeWithOr(e, rhs)
}

func (e Choose) Multiply(rhs Expr) And {
	return mergeWithAnd(e, rhs)
}

func (e Choose) Quorums() chan ExprSet {
	chnl := make(chan ExprSet)
	sets := make([]ExprSet, 0)

	for _, combo := range combinations(e.Es, e.K) {
		combinedQuorums := make([][]interface{}, 0)
		for _, c := range combo {
			quorums := make([]interface{}, 0)

			for q := range c.Quorums() {
				quorums = append(quorums, q)
			}
			combinedQuorums = append(combinedQuorums, quorums)

		}
		for _, s := range product(combinedQuorums...) {
			set := make(ExprSet)
			for _, t := range s {
				set = mergeExprSets(set, t.(ExprSet))
			}
			sets = append(sets, set)
		}
	}

	go func() {
		for _, set := range sets {
			chnl <- set
		}
		close(chnl)
	}()

	return chnl
}

func (e Choose) IsQuorum(xs ExprSet) bool {
	sum := 0
	for _, es := range e.Es {
		if es.IsQuorum(xs) {
			sum += 1
		}
	}
	return sum >= e.K
}

func (e Choose) GetNodes() NodeSet {
	var final = make(NodeSet)

	for _, es := range e.Es {
		for n := range es.GetNodes() {
			final[n] = true
		}
	}
	return final
}

func (e Choose) NumLeaves() uint {
	total := uint(0)

	for _, es := range e.Es {
		total += es.NumLeaves()
	}

	return total
}

func (e Choose) MinFailures() uint {
	var exprs = e.Es

	var subFailures []int

	for _, expr := range exprs {
		subFailures = append(subFailures, int(expr.MinFailures()))
	}

	sort.Ints(subFailures)

	sortedSubset := subFailures[:len(subFailures)-e.K+1]
	total := 0

	for _, v := range sortedSubset {
		total += v
	}

	return uint(total)
}

func (e Choose) Resilience() uint {
	if e.DupFree() {
		return e.MinFailures() - 1
	}

	qs := make([]ExprSet, 0)

	for q := range e.Quorums() {
		qs = append(qs, q)
	}

	return minHittingSet(qs) - 1.0
}

func (e Choose) DupFree() bool {
	return uint(len(e.GetNodes())) == e.NumLeaves()
}

func (e Choose) String() string {
	if len(e.Es) == 0 {
		return "()"
	}
	var sb strings.Builder

	sb.WriteString("(")
	sb.WriteString(e.Es[0].String())

	for _, v := range e.Es[1:] {
		sb.WriteString(" * ")
		sb.WriteString(v.String())
	}

	sb.WriteString(")")

	return sb.String()
}

func (e Choose) GetType() string {
	return "Choose"
}

func (e Choose) GetExprs() []Expr {
	return e.Es
}

func (e Choose) Dual() Expr {
	dualExprs := make([]Expr, 0)
	for _, es := range e.Es {
		dualExprs = append(dualExprs, es.Dual())
	}
	return Choose{Es: dualExprs, K: len(e.Es) - e.K + 1}
}

// mergeWithOr returns a Or expression between two input expressions.
func mergeWithOr(lhs Expr, rhs Expr) Or {
	if reflect.TypeOf(lhs).Name() == "Or" && reflect.TypeOf(rhs).String() == "Or" {
		return Or{append(lhs.GetExprs(), rhs.GetExprs()...)}
	} else if reflect.TypeOf(lhs).Name() == "Or" {
		return Or{append(lhs.GetExprs(), rhs)}
	} else if reflect.TypeOf(rhs).String() == "Or" {
		return Or{append([]Expr{lhs}, rhs.GetExprs()...)}
	} else {
		return Or{[]Expr{lhs, rhs}}
	}
}

// mergeWithAnd returns an And expression between two input expressions.
func mergeWithAnd(lhs Expr, rhs Expr) And {
	if reflect.TypeOf(lhs).Name() == "And" && reflect.TypeOf(rhs).String() == "And" {
		return And{append(lhs.GetExprs(), rhs.GetExprs()...)}
	} else if reflect.TypeOf(lhs).Name() == "And" {
		return And{append(lhs.GetExprs(), rhs)}
	} else if reflect.TypeOf(rhs).String() == "And" {
		return And{append([]Expr{lhs}, rhs.GetExprs()...)}
	} else {
		return And{[]Expr{lhs, rhs}}
	}
}

// mergeExprSets returns a merge between multiple ExprSet.
func mergeExprSets(maps ...ExprSet) ExprSet {
	result := make(ExprSet)
	for _, m := range maps {
		for k, v := range m {
			result[k] = v
		}
	}
	return result
}

//product returns the cartesian product between a list of inputs.
func product(sets ...[]interface{}) [][]interface{} {
	result := make([][]interface{}, 0)
	nextIndex := func(ix []int, lens func(i int) int) {
		for j := len(ix) - 1; j >= 0; j-- {
			ix[j]++
			if j == 0 || ix[j] < lens(j) {
				return
			}
			ix[j] = 0
		}
	}
	lens := func(i int) int { return len(sets[i]) }

	for ix := make([]int, len(sets)); ix[0] < lens(0); nextIndex(ix, lens) {
		var r []interface{}
		for j, k := range ix {
			r = append(r, sets[j][k])
		}
		result = append(result, r)
	}

	return result
}

//combinations returns n combinations given a list of input Expr
func combinations(set []Expr, n int) (subsets [][]Expr) {
	length := len(set)

	if n > len(set) {
		n = len(set)
	}

	for subsetBits := 1; subsetBits < (1 << length); subsetBits++ {
		if n > 0 && bits.OnesCount(uint(subsetBits)) != n {
			continue
		}

		var ss []Expr

		for object := 0; object < length; object++ {
			if (subsetBits>>object)&1 == 1 {
				ss = append(ss, set[object])
			}
		}
		subsets = append(subsets, ss)
	}
	return subsets
}

//exprSetToArr given an input ExprSet returns an []Expr.
func exprSetToArr(input ExprSet) []Expr {
	result := make([]Expr, 0)

	for k := range input {
		result = append(result, k)
	}

	return result
}

func minHittingSet(quorums []ExprSet) uint {

	keys := make([]Expr, 0)

	def := lpDefinition{}
	def.Vars = make([]float64, 0)
	def.Constraints = make([][2]float64, 0)
	def.Objectives = make([][]float64, 0)

	simp := clp.NewSimplex()

	uniqueKeys := make(map[Expr]float64)

	for _, xs := range quorums {
		for k := range xs {
			if _, exists := uniqueKeys[k]; !exists {
				keys = append(keys, k)
			}

			uniqueKeys[k] = 1.0
		}
	}

	for range keys {
		def.Vars = append(def.Vars, 1.0)
	}

	for range keys {
		constr := [2]float64{0, 1}
		def.Constraints = append(def.Constraints, constr)
	}

	for _, xs := range quorums {
		obj := make([]float64, 0)
		obj = append(obj, 1)

		for _, k := range keys {
			if _, exists := xs[k]; exists {
				obj = append(obj, 1)
			} else {
				obj = append(obj, 0)
			}
		}

		obj = append(obj, math.Inf(1))
		def.Objectives = append(def.Objectives, obj)
	}

	// Set up the optimization problem.
	simp.EasyLoadDenseProblem(
		def.Vars,
		def.Constraints,
		def.Objectives)

	simp.SetOptimizationDirection(clp.Minimize)

	// Solve the optimization problem.
	status := simp.Primal(clp.NoValuesPass, clp.NoStartFinishOptions)
	soln := simp.PrimalColumnSolution()

	if status != clp.Optimal {
		fmt.Println("Error")
	}

	result := uint(0)

	for _, v := range soln {
		result += uint(math.Round(v))
	}

	return result
}
