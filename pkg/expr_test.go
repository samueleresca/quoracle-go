package pkg

import (
	"fmt"
	"gotest.tools/assert"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func TestNode(t *testing.T) {
	a, b, c := NewNode("a"), NewNode("b"), NewNode("c")
	assert.Assert(t, a.String() == "a")
	assert.Assert(t, b.String() == "b")
	assert.Assert(t, c.String() == "c")
}

func TestQuorums(t *testing.T) {
	assertQuorums := func(e Quorum, xs [][]string) {
		actual := make([]string, 0)

		for q := range e.Quorums() {
			var tmp []string

			for n := range q {
				tmp = append(tmp, n.String())
			}
			sort.Strings(tmp)
			actual = append(actual, strings.Join(tmp, ""))
		}

		var expected []string

		for _, x := range xs {
			sort.Strings(x)
			expected = append(expected, strings.Join(x, ""))
		}

		sort.Strings(actual)
		sort.Strings(expected)

		assert.Assert(t, reflect.DeepEqual(actual, expected) == true, fmt.Sprintf("assertQuorums - Actual: %v | Expected  %v", actual, expected))
	}

	a, b, c, d, e, f := NewNode("a"), NewNode("b"), NewNode("c"), NewNode("d"), NewNode("e"), NewNode("f")
	assert.Assert(t, a.Add(b).Add(c).String() == "(a + b + c)")
	assert.Assert(t, a.Multiply(b).Multiply(c).String() == "(a * b * c)")
	assert.Assert(t, a.Add(b.Multiply(c)).String() == "(a + (b * c))")

	tests := []struct {
		expr     GenericExpr
		expected [][]string
	}{
		{(a.Multiply(b)).Multiply(c), [][]string{{"a", "b", "c"}}},
		{a.Add(b).Add(c), [][]string{{"a"}, {"b"}, {"c"}}},
		{And{Es: []GenericExpr{And{Es: []GenericExpr{a, b}}, c}}, [][]string{{"a", "b", "c"}}},
		{a.Add(b.Multiply(c)), [][]string{{"a"}, {"b", "c"}}},
		{a.Multiply(a).Multiply(a), [][]string{{"a"}}},
		{a.Multiply(a.Add(b)), [][]string{{"a"}, {"a", "b"}}},
		{a.Multiply(a.Add(b)), [][]string{{"a"}, {"a", "b"}}},
		{a.Add(b).Multiply(a.Add(c)), [][]string{{"a"}, {"a", "c"}, {"a", "b"}, {"b", "c"}}},
	}

	for _, tt := range tests {
		assertQuorums(tt.expr, tt.expected)
	}

	expr, _ := NewChoose(1, []GenericExpr{a, b, c})
	assertQuorums(expr, [][]string{{"a"}, {"b"}, {"c"}})

	expr, _ = NewChoose(2, []GenericExpr{a, b, c})
	assertQuorums(expr, [][]string{{"a", "c"}, {"a", "b"}, {"b", "c"}})

	expr, _ = NewChoose(3, []GenericExpr{a, b, c})
	assertQuorums(expr, [][]string{{"a", "b", "c"}})

	expr1, _ := NewChoose(2, []GenericExpr{a, b, c})
	expr2, _ := NewChoose(2, []GenericExpr{d, e, f})
	expr3, _ := NewChoose(2, []GenericExpr{a, c, e})

	expr, _ = NewChoose(2, []GenericExpr{expr1, expr2, expr3})

	assertQuorums(expr, [][]string{{"a", "b", "d", "e"}, {"a", "b", "d", "f"}, {"a", "b", "e", "f"},
		{"a", "c", "d", "e"}, {"a", "c", "d", "f"}, {"a", "c", "e", "f"},
		{"b", "c", "d", "e"}, {"b", "c", "d", "f"}, {"b", "c", "e", "f"},
		{"a", "b", "c"}, {"a", "b", "e"}, {"a", "b", "c", "e"},
		{"a", "c"}, {"a", "c", "e"}, {"a", "c", "e"},
		{"b", "c", "a"}, {"b", "c", "a", "e"}, {"b", "c", "e"},
		{"d", "e", "a", "c"}, {"d", "a", "e"}, {"d", "e", "c"},
		{"d", "f", "a", "c"}, {"d", "f", "a", "e"}, {"d", "f", "c", "e"},
		{"e", "f", "a", "c"}, {"e", "f", "a"}, {"e", "f", "c"}})
}

func TestIsQuorum(t *testing.T) {

	a, b, c, d := NewNode("a"), NewNode("b"), NewNode("c"), NewNode("d")

	expr := a.Add(b).Add(c)

	tests := []struct {
		expr    Quorum
		expected ExprSet
		isQuorum bool
	}{
		{expr, ExprSet{Node{Name: "a"}: true}, true},
		{expr, ExprSet{Node{Name: "b"}: true}, true},
		{expr, ExprSet{Node{Name: "c"}: true}, true},
		{expr, ExprSet{Node{Name: "a"}: true, Node{Name: "b"}: true}, true},
		{expr, ExprSet{Node{Name: "a"}: true, Node{Name: "c"}: true}, true},
		{expr, ExprSet{Node{Name: "b"}: true, Node{Name: "c"}: true}, true},
		{expr, ExprSet{}, false},
		{expr, ExprSet{Node{Name: "x"}: true}, false},
	}

	for _, tt := range tests {
		assert.Assert(t, tt.expr.IsQuorum(tt.expected) == tt.isQuorum)
	}

	chooseExp, _ := NewChoose(2, []GenericExpr{a, b, c})

	tests = []struct {
		expr    Quorum
		expected ExprSet
		isQuorum bool
	}{
		{chooseExp, ExprSet{Node{Name: "a"}: true, Node{Name: "b"}: true, Node{Name: "c"}: true}, true},
		{chooseExp, ExprSet{Node{Name: "a"}: true, Node{Name: "b"}: true, Node{Name: "c"}: true, Node{Name: "x"}: true}, true},
		{chooseExp, ExprSet{}, false},
		{chooseExp, ExprSet{Node{Name: "a"}: true}, false},
		{chooseExp, ExprSet{Node{Name: "b"}: true}, false},
		{chooseExp, ExprSet{Node{Name: "c"}: true}, false},
		{chooseExp, ExprSet{Node{Name: "x"}: true}, false},
		{chooseExp, ExprSet{Node{Name: "x"}: true}, false},
		{chooseExp, ExprSet{Node{Name: "a"}: true, Node{Name: "b"}: true}, true},
		{chooseExp, ExprSet{Node{Name: "a"}: true, Node{Name: "c"}: true}, true},
		{chooseExp, ExprSet{Node{Name: "b"}: true, Node{Name: "c"}: true}, true},
	}

	for _, tt := range tests {
		assert.Assert(t, tt.expr.IsQuorum(tt.expected) == tt.isQuorum)
	}

	exprAnd := a.Multiply(b).Multiply(c)

	tests = []struct {
		expr    Quorum
		expected ExprSet
		isQuorum bool
	}{
		{exprAnd, ExprSet{Node{Name: "a"}: true, Node{Name: "b"}: true, Node{Name: "c"}: true}, true},
		{exprAnd, ExprSet{Node{Name: "a"}: true, Node{Name: "b"}: true, Node{Name: "c"}: true, Node{Name: "x"}: true}, true},
		{exprAnd, ExprSet{}, false},
		{exprAnd, ExprSet{Node{Name: "a"}: true}, false},
		{exprAnd, ExprSet{Node{Name: "b"}: true}, false},
		{exprAnd, ExprSet{Node{Name: "c"}: true}, false},
		{exprAnd, ExprSet{Node{Name: "a"}: true, Node{Name: "b"}: true}, false},
		{exprAnd, ExprSet{Node{Name: "a"}: true, Node{Name: "c"}: true}, false},
		{exprAnd, ExprSet{Node{Name: "b"}: true, Node{Name: "c"}: true}, false},
		{exprAnd, ExprSet{Node{Name: "x"}: true}, false},
		{exprAnd, ExprSet{Node{Name: "a"}: true, Node{Name: "x"}: true}, false},
	}

	for _, tt := range tests {
		assert.Assert(t, tt.expr.IsQuorum(tt.expected) == tt.isQuorum)
	}

	exprp := a.Add(b).Multiply(c.Add(d))

	tests = []struct {
		expr    Quorum
		expected ExprSet
		isQuorum bool
	}{
		{exprp, ExprSet{Node{Name: "a"}: true, Node{Name: "c"}: true}, true},
		{exprp, ExprSet{Node{Name: "a"}: true, Node{Name: "c"}: true}, true},
		{exprp, ExprSet{Node{Name: "a"}: true, Node{Name: "d"}: true}, true},
		{exprp, ExprSet{Node{Name: "b"}: true, Node{Name: "d"}: true}, true},
		{exprp, ExprSet{Node{Name: "a"}: true, Node{Name: "b"}: true, Node{Name: "d"}: true}, true},
		{exprp, ExprSet{Node{Name: "b"}: true, Node{Name: "c"}: true, Node{Name: "d"}: true}, true},
		{exprp, ExprSet{Node{Name: "b"}: true, Node{Name: "c"}: true, Node{Name: "d"}: true}, true},
		{exprp, ExprSet{Node{Name: "a"}: true, Node{Name: "b"}: true, Node{Name: "d"}: true}, true},
		{exprp, ExprSet{Node{Name: "a"}: true, Node{Name: "c"}: true, Node{Name: "d"}: true}, true},
		{exprp, ExprSet{Node{Name: "a"}: true, Node{Name: "b"}: true, Node{Name: "c"}: true, Node{Name: "d"}: true}, true},
		{exprp, ExprSet{Node{Name: "a"}: true}, false},
		{exprp, ExprSet{Node{Name: "b"}: true}, false},
		{exprp, ExprSet{Node{Name: "a"}: true}, false},
		{exprp, ExprSet{Node{Name: "c"}: true}, false},
		{exprp, ExprSet{Node{Name: "d"}: true}, false},
		{exprp, ExprSet{Node{Name: "a"}: true, Node{Name: "b"}: true}, false},
		{exprp, map[GenericExpr]bool{Node{Name: "c"}: true, Node{Name: "d"}: true}, false},
		{exprp, map[GenericExpr]bool{Node{Name: "x"}: true}, false},
	}

	for _, tt := range tests {
		assert.Assert(t, tt.expr.IsQuorum(tt.expected) == tt.isQuorum)
	}
}

func TestResilience(t *testing.T) {

	a, b, c, d, e, f := NewNode("a"), NewNode("b"), NewNode("c"), NewNode("d"), NewNode("e"), NewNode("f")

	assertResilience := func(expr ResilienceCalculator, n uint) {
		actual := expr.Resilience()
		assert.Assert(t, actual == n, fmt.Sprintf("Actual: %d | Expected  %d", actual, n))
	}

	tests := []struct {
		expr     GenericExpr
		expected uint
	}{
		{a, 0},
		{a.Add(b), 1},
		{a.Add(b).Add(c), 2},
		{a.Add(b).Add(c).Add(d), 3},
		{a.Multiply(b), 0},
		{a.Multiply(b).Multiply(c), 0},
		{a.Multiply(b).Multiply(c).Multiply(d), 0},
		{a.Add(b).Multiply(c.Add(d)), 1},
		{a.Add(b).Add(c).Multiply(d.Add(e).Add(f)), 2},
		{(a.Add(b).Add(c)).Multiply(a.Add(e).Add(f)), 2},
		{a.Add(a).Add(c).Multiply(d.Add(e).Add(f)), 1},
		{(a.Add(a).Add(a)).Multiply(d.Add(e).Add(f)), 0},
		{(a.Multiply(b)).Add(b.Multiply(c)).Add(a.Multiply(d)).Add(a.Multiply(d).Multiply(e)), 1},
	}

	for _, tt := range tests {
		assertResilience(tt.expr, tt.expected)
	}

	testsChoose := []struct {
		k        int
		exprs    []GenericExpr
		expected uint
	}{
		{2, []GenericExpr{a, b, c}, 1},
		{2, []GenericExpr{a, b, c, d, e}, 3},
		{3, []GenericExpr{a, b, c, d, e}, 2},
		{4, []GenericExpr{a, b, c, d, e}, 1},
		{2, []GenericExpr{a.Add(b).Add(c), d.Add(e), f}, 2},
		{2, []GenericExpr{a.Multiply(b), a.Multiply(c), d}, 0},
		{2, []GenericExpr{a.Add(b), a.Add(c), a.Add(d)}, 2},
	}

	for _, tt := range testsChoose {
		expr, _ := NewChoose(tt.k, tt.exprs)
		assertResilience(expr, tt.expected)
	}
}

func TestDual(t *testing.T) {

	assertDual := func(x DualOperator, y GenericExpr) {
		assert.DeepEqual(t, x.Dual(), y)
	}

	a, b, c, d, e := NewNode("a"), NewNode("b"), NewNode("c"), NewNode("d"), NewNode("e")

	tests := []struct {
		expr1  GenericExpr
		expr2  GenericExpr
		isDual bool
	}{
		{a, a, true},
		{a.Add(b), a.Multiply(b), true},
		{a.Add(a), a.Multiply(a), true},
		{(a.Add(b)).Multiply(c.Add(d)), (a.Multiply(b)).Add(c.Multiply(d)), true},
		{(a.Add(b)).Multiply(a.Add(d)), (a.Multiply(b)).Add(a.Multiply(d)), true},
		{(a.Add(b)).Multiply(a.Add(a)), (a.Multiply(b)).Add(a.Multiply(a)), true},
		{(a.Add(a)).Multiply(a.Add(a)), (a.Multiply(a)).Add(a.Multiply(a)), true},
		{(a.Add(a.Multiply(b))).Add((c.Multiply(d)).Add(a)), (a.Multiply(a.Add(b))).Multiply((c.Add(d)).Multiply(a)), true},
	}

	for _, tt := range tests {
		assertDual(tt.expr1, tt.expr2)
	}

	testsChoose := []struct {
		k1    int
		k2    int
		expr1 []GenericExpr
		expr2 []GenericExpr
	}{
		{2, 2, []GenericExpr{a, b, c}, []GenericExpr{a, b, c}},
		{2, 2, []GenericExpr{a.Add(b), c.Add(d), e}, []GenericExpr{a.Multiply(b), c.Multiply(d), e}},
		{3, 3, []GenericExpr{a, b, c, d, e}, []GenericExpr{a, b, c, d, e}},
		{2, 4, []GenericExpr{a, b, c, d, e}, []GenericExpr{a, b, c, d, e}},
		{4, 2, []GenericExpr{a, b, c, d, e}, []GenericExpr{a, b, c, d, e}},
	}

	for _, tt := range testsChoose {
		expr1, _ := NewChoose(tt.k1, tt.expr1)
		expr2, _ := NewChoose(tt.k2, tt.expr2)

		assertDual(expr1, expr2)
	}
}

func TestDupFree(t *testing.T) {
	a, b, c, d, e, f := NewNode("a"), NewNode("b"), NewNode("c"), NewNode("d"), NewNode("e"), NewNode("f")

	tests := []struct {
		expr      DuplicateChecker
		isDupFree bool
	}{
		{a, true},
		{a.Add(b), true},
		{a.Multiply(b), true},
		{a.Multiply(b).Add(c), true},
		{(a.Add(b)).Multiply(c.Add(d.Multiply(e))), true},
		{a.Add(a), false},
		{a.Multiply(a), false},
		{a.Multiply(b.Add(a)), false},
		{(a.Add(b)).Multiply(c.Add(d.Multiply(a))), false},
	}

	for _, tt := range tests {
		assert.Assert(t, tt.expr.DupFree() == tt.isDupFree)
	}

	testsChoose := []struct {
		k         int
		exprs     []GenericExpr
		isDupFree bool
	}{
		{2, []GenericExpr{a, b, c}, true},
		{2, []GenericExpr{a.Multiply(b), c, d.Add(e).Add(f)}, true},
		{3, []GenericExpr{a, b, c, d, e}, true},
		{2, []GenericExpr{a, b, a}, false},
		{3, []GenericExpr{a, b, c, d, a}, false},
	}

	for _, tt := range testsChoose {
		expr, _ := NewChoose(tt.k, tt.exprs)
		assert.Assert(t, expr.DupFree() == tt.isDupFree)
	}
}
