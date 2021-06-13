package pkg

import (
	"fmt"
	"github.com/lanl/clp"
	"math"
	"math/bits"
	"sort"
	"strings"
)

type ExprSet = map[GenericExpr]bool

type NodeSet = map[Node]bool

type ExprOperator interface {
	Add(expr GenericExpr) Or
	Multiply(expr GenericExpr) And
	Expr() string
}

type GenericExpr interface {
	ExprOperator
	Quorums() chan ExprSet
	IsQuorum(set ExprSet) bool
	Nodes() NodeSet
	NumLeaves() uint
	DupFreeMinFailures() uint
	Resilience() uint
	DupFree() bool
	String() string
	GetEs() []GenericExpr
	Dual() GenericExpr
}

// Node in a quorum
type Node struct {
	Name          string
	ReadCapacity  *uint
	WriteCapacity *uint
	Latency       *uint
}

func DefNode(name string) Node {
	node := Node{}
	node.Name = name

	initialValue := uint(1)
	node.ReadCapacity = &initialValue
	node.WriteCapacity = &initialValue

	return node
}

func DefNodeWithCapacityAndLatency(name string, readCapacity uint, writeCapacity uint, latency uint) Node {
	node := Node{}

	node.Name = name
	node.ReadCapacity = &readCapacity
	node.WriteCapacity = &writeCapacity
	node.Latency = &latency

	return node
}

func DefNodeWithCapacity(name string, readCapacity uint, writeCapacity uint) Node {
	node := Node{}

	node.Name = name
	node.ReadCapacity = &readCapacity
	node.WriteCapacity = &writeCapacity

	return node
}

func DefNodeWithLatency(name string, latency uint) Node {
	node := Node{}

	node.Name = name
	initialValue := uint(1)
	node.ReadCapacity = &initialValue
	node.WriteCapacity = &initialValue
	node.Latency = &latency

	return node
}

func (n Node) Add(expr GenericExpr) Or {
	return orExpr(n, expr)
}

func (n Node) Multiply(expr GenericExpr) And {
	return andExpr(n, expr)
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
	var found = false
	for k := range xs {
		if n.String() == k.String() {
			found = true
			return found
		}
	}
	return found
}

func (n Node) Nodes() NodeSet {
	return NodeSet{n: true}
}

func (n Node) NumLeaves() uint {
	return 1
}

func (n Node) DupFreeMinFailures() uint {
	return 1
}

func (n Node) Resilience() uint {
	if n.DupFree() {
		return n.DupFreeMinFailures() - 1
	}

	qs := make([]ExprSet, 0)

	for q := range n.Quorums() {
		qs = append(qs, q)
	}

	return minHittingSet(qs) - 1.0
}

func (n Node) DupFree() bool {
	return uint(len(n.Nodes())) == n.NumLeaves()
}

func (n Node) String() string {
	return n.Name
}

func (n Node) Expr() string {
	return "Node"
}

func (n Node) GetEs() []GenericExpr {
	return []GenericExpr{n}
}

func (n Node) Dual() GenericExpr {
	return n
}

// Or represents an logical or expression
type Or struct {
	Es []GenericExpr
}

func (e Or) Add(rhs GenericExpr) Or {
	return orExpr(e, rhs)
}

func (e Or) Multiply(rhs GenericExpr) And {
	return andExpr(e, rhs)
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

func (e Or) Nodes() NodeSet {
	var final = make(NodeSet)

	for _, es := range e.Es {
		for n := range es.Nodes() {
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

func (e Or) DupFreeMinFailures() uint {
	total := uint(0)

	for _, es := range e.Es {
		total += es.DupFreeMinFailures()
	}

	return total
}

func (e Or) Resilience() uint {
	if e.DupFree() {
		return e.DupFreeMinFailures() - 1
	}

	qs := make([]ExprSet, 0)

	for q := range e.Quorums() {
		qs = append(qs, q)
	}

	return minHittingSet(qs) - 1.0
}

func (e Or) DupFree() bool {
	return uint(len(e.Nodes())) == e.NumLeaves()
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

func (e Or) Expr() string {
	return "Or"
}

func (e Or) GetEs() []GenericExpr {
	return e.Es
}

func (e Or) Dual() GenericExpr {
	dualExprs := make([]GenericExpr, 0)
	for _, es := range e.Es {
		dualExprs = append(dualExprs, es.Dual())
	}
	return And{Es: dualExprs}
}

// And represents a logical and operation in the quorums
type And struct {
	Es []GenericExpr
}

func (e And) Add(rhs GenericExpr) Or {
	return orExpr(e, rhs)
}

func (e And) Multiply(rhs GenericExpr) And {
	return andExpr(e, rhs)
}

func (e And) Quorums() chan ExprSet {
	chnl := make(chan ExprSet)
	flatQuorums := make([][]GenericExpr, 0)

	for _, es := range e.Es {
		tmp := make(ExprSet, 0)

		for q := range es.Quorums() {
			tmp = mergeGenericExprSets(tmp, q)
		}
		flatQuorums = append(flatQuorums, exprMapToList(tmp))
	}

	go func() {
		for _, sets := range product(flatQuorums...) {
			chnl <- exprListToMap(sets)
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

func (e And) Nodes() NodeSet {
	var final = make(NodeSet)

	for _, es := range e.Es {
		for n := range es.Nodes() {
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

func (e And) DupFreeMinFailures() uint {
	var exprs = e.Es
	var min = exprs[0].DupFreeMinFailures()

	for _, expr := range exprs {
		if min > expr.DupFreeMinFailures() {
			min = expr.DupFreeMinFailures()
		}
	}
	return min
}

func (e And) Resilience() uint {
	if e.DupFree() {
		return e.DupFreeMinFailures() - 1
	}

	qs := make([]ExprSet, 0)

	for q := range e.Quorums() {
		qs = append(qs, q)
	}

	return minHittingSet(qs) - 1
}

func (e And) DupFree() bool {
	return uint(len(e.Nodes())) == e.NumLeaves()
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

func (e And) Expr() string {
	return "And"
}

func (e And) GetEs() []GenericExpr {
	return e.Es
}

func (e And) Dual() GenericExpr {
	dualExprs := make([]GenericExpr, 0)
	for _, es := range e.Es {
		dualExprs = append(dualExprs, es.Dual())
	}
	return Or{Es: dualExprs}
}

// Choose represents a logical
type Choose struct {
	Es []GenericExpr
	K  int
}

func NewChoose(k int, es []GenericExpr) (Choose, error) {
	if k <= 0 || k > len(es) {
		return Choose{}, fmt.Errorf("k must be in the range [1, %d]", len(es))
	}

	return Choose{Es: es, K: k}, nil
}

func DefChoose(k int, es []GenericExpr) (GenericExpr, error) {
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

	return NewChoose(k, es)
}

func (e Choose) Add(rhs GenericExpr) Or {
	return orExpr(e, rhs)
}

func (e Choose) Multiply(rhs GenericExpr) And {
	return andExpr(e, rhs)
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
		for _, s := range productInterfaces(combinedQuorums...) {
			set := make(ExprSet)
			for _, t := range s {
				set = mergeGenericExprSets(set, t.(ExprSet))
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

func (e Choose) Nodes() NodeSet {
	var final = make(NodeSet)

	for _, es := range e.Es {
		for n := range es.Nodes() {
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

func (e Choose) DupFreeMinFailures() uint {
	var exprs = e.Es

	var subFailures []int

	for _, expr := range exprs {
		subFailures = append(subFailures, int(expr.DupFreeMinFailures()))
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
		return e.DupFreeMinFailures() - 1
	}

	qs := make([]ExprSet, 0)

	for q := range e.Quorums() {
		qs = append(qs, q)
	}

	return minHittingSet(qs) - 1.0
}

func (e Choose) DupFree() bool {
	return uint(len(e.Nodes())) == e.NumLeaves()
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

func (e Choose) Expr() string {
	return "Choose"
}

func (e Choose) GetEs() []GenericExpr {
	return e.Es
}

func (e Choose) Dual() GenericExpr {
	dualExprs := make([]GenericExpr, 0)
	for _, es := range e.Es {
		dualExprs = append(dualExprs, es.Dual())
	}
	return Choose{Es: dualExprs, K: len(e.Es) - e.K + 1}
}

// Additional methods

func orExpr(lhs GenericExpr, rhs GenericExpr) Or {
	if lhs.Expr() == "Or" && rhs.Expr() == "Or" {
		return Or{append(lhs.GetEs(), rhs.GetEs()...)}
	} else if lhs.Expr() == "Or" {
		return Or{append(lhs.GetEs(), rhs)}
	} else if rhs.Expr() == "Or" {
		return Or{append([]GenericExpr{lhs}, rhs.GetEs()...)}
	} else {
		return Or{[]GenericExpr{lhs, rhs}}
	}
}

func andExpr(lhs GenericExpr, rhs GenericExpr) And {
	if lhs.Expr() == "And" && rhs.Expr() == "And" {
		return And{append(lhs.GetEs(), rhs.GetEs()...)}
	} else if lhs.Expr() == "And" {
		return And{append(lhs.GetEs(), rhs)}
	} else if rhs.Expr() == "And" {
		return And{append([]GenericExpr{lhs}, rhs.GetEs()...)}
	} else {
		return And{[]GenericExpr{lhs, rhs}}
	}
}

func mergeGenericExprSets(maps ...ExprSet) ExprSet {
	result := make(ExprSet)
	for _, m := range maps {
		for k, v := range m {
			result[k] = v
		}
	}
	return result
}

// Cartesian product of lists, see: https://www.programmersought.com/article/95476401483/
func product(sets ...[]GenericExpr) [][]GenericExpr {
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
	var product [][]GenericExpr
	for ix := make([]int, len(sets)); ix[0] < lens(0); nextIndex(ix, lens) {
		var r []GenericExpr

		for j, k := range ix {
			r = append(r, sets[j][k])
		}
		product = append(product, r)
	}
	return product
}

func productInterfaces(sets ...[]interface{}) [][]interface{} {
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

// Returns N combinations of GenericExpr
func combinations(set []GenericExpr, n int) (subsets [][]GenericExpr) {
	length := len(set)

	if n > len(set) {
		n = len(set)
	}

	for subsetBits := 1; subsetBits < (1 << length); subsetBits++ {
		if n > 0 && bits.OnesCount(uint(subsetBits)) != n {
			continue
		}

		var ss []GenericExpr

		for object := 0; object < length; object++ {
			if (subsetBits>>object)&1 == 1 {
				ss = append(ss, set[object])
			}
		}
		subsets = append(subsets, ss)
	}
	return subsets
}

func exprMapToList(input ExprSet) []GenericExpr {
	result := make([]GenericExpr, 0)

	for k := range input {
		result = append(result, k)
	}

	return result
}

func exprListToMap(input []GenericExpr) ExprSet {
	result := make(ExprSet)

	for _, k := range input {
		result[k] = true
	}

	return result
}

func minHittingSet(sets []ExprSet) uint {

	xVars := make(map[GenericExpr]float64)
	x := make([]float64, 0)
	constraints := make([][2]float64, 0)
	obj := make([][]float64, 0)
	//pinf := math.Inf(1)
	simp := clp.NewSimplex()

	for _, xs := range sets {
		for k := range xs {
			xVars[k] = 1.0
		}
	}

	for range xVars {
		x = append(x, 1.0)
	}

	for range xVars {
		tmp := [][2]float64{{0, 1}}
		constraints = append(constraints, tmp...)
	}

	for _, xs := range sets {
		tmp := make([]float64, 0)
		tmp = append(tmp, 1)

		for k := range xVars {
			if _, ok := xs[k]; ok {
				tmp = append(tmp, 1)
			} else {
				tmp = append(tmp, 0)
			}
		}
		tmp = append(tmp, math.Inf(1))
		obj = append(obj, tmp)
	}

	// Set up the optimization problem.

	simp.EasyLoadDenseProblem(
		x,
		constraints,
		obj)

	simp.SetOptimizationDirection(clp.Minimize)

	// Solve the optimization problem.
	status := simp.Primal(clp.NoValuesPass, clp.NoStartFinishOptions)
	soln := simp.PrimalColumnSolution()
	fmt.Println(soln)

	if status != clp.Optimal {
		fmt.Println("Error")
	}

	result := uint(0)
	for _, v := range soln {
		result += uint(math.Round(v))
	}
	return result
}
