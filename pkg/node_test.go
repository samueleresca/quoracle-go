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
	a, b, c := DefNode("a"), DefNode("b"), DefNode("c")
	assert.Assert(t, a.String() == "a")
	assert.Assert(t, b.String() == "b")
	assert.Assert(t, c.String() == "c")
}

func TestQuorums(t *testing.T) {
	assertQuorums := func(e GenericExpr, xs [][]string) {
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

	a, b, c, d, e, f := DefNode("a"), DefNode("b"), DefNode("c"), DefNode("d"), DefNode("e"), DefNode("f")
	assert.Assert(t, a.Add(b).Add(c).String() == "(a + b + c)")
	assert.Assert(t, a.Multiply(b).Multiply(c).String() == "(a * b * c)")
	assert.Assert(t, a.Add(b.Multiply(c)).String() == "(a + (b * c))")

	assertQuorums(a.Multiply(b).Multiply(c), [][]string{{"a", "b", "c"}})
	assertQuorums(a.Add(b).Add(c), [][]string{{"a"}, {"b"}, {"c"}})

	assertQuorums(a.Add(b.Multiply(c)), [][]string{{"a"}, {"b", "c"}})
	assertQuorums(a.Multiply(a).Multiply(a), [][]string{{"a"}})
	assertQuorums(a.Multiply(a.Add(b)), [][]string{{"a"}, {"a", "b"}})
	assertQuorums(a.Multiply(a.Add(b)), [][]string{{"a"}, {"a", "b"}})
	assertQuorums(a.Add(b).Multiply(a.Add(c)), [][]string{{"a"}, {"a", "c"}, {"a", "b"}, {"b", "c"}})

	expr, _ := DefChoose(1, []GenericExpr{a, b, c})
	assertQuorums(expr, [][]string{{"a"}, {"b"}, {"c"}})

	expr, _ = DefChoose(2, []GenericExpr{a, b, c})
	assertQuorums(expr, [][]string{{"a", "c"}, {"a", "b"}, {"b", "c"}})

	expr, _ = DefChoose(3, []GenericExpr{a, b, c})
	assertQuorums(expr, [][]string{{"a", "b", "c"}})

	expr1, _ := DefChoose(2, []GenericExpr{a, b, c})
	expr2, _ := DefChoose(2, []GenericExpr{d, e, f})
	expr3, _ := DefChoose(2, []GenericExpr{a, c, e})

	expr, _ = DefChoose(2, []GenericExpr{expr1, expr2, expr3})

	assertQuorums(expr, [][]string{{"a", "b", "d", "e"}, {"a", "b", "d", "f"}, {"a", "b", "e", "f"},
		{"a", "c", "d", "e"}, {"a", "c", "d", "f"}, {"a", "c", "e", "f"},
		{"b", "c", "d", "e"}, {"b", "c", "d", "f"}, {"b", "c", "e", "f"},
		{"a", "b", "a", "c"}, {"a", "b", "a", "e"}, {"a", "b", "c", "e"},
		{"a", "c", "a", "c"}, {"a", "c", "a", "e"}, {"a", "c", "c", "e"},
		{"b", "c", "a", "c"}, {"b", "c", "a", "e"}, {"b", "c", "c", "e"},
		{"d", "e", "a", "c"}, {"d", "e", "a", "e"}, {"d", "e", "c", "e"},
		{"d", "f", "a", "c"}, {"d", "f", "a", "e"}, {"d", "f", "c", "e"},
		{"e", "f", "a", "c"}, {"e", "f", "a", "e"}, {"e", "f", "c", "e"}})
}

func TestIsQuorum(t *testing.T) {

	a, b, c, d := DefNode("a"), DefNode("b"), DefNode("c"), DefNode("d")

	assertIsQuorum := func(expr GenericExpr, q map[GenericExpr]bool) {
		assert.Assert(t, expr.IsQuorum(q) == true)
	}

	assertIsNotQuorum := func(expr GenericExpr, q map[GenericExpr]bool) {
		assert.Assert(t, expr.IsQuorum(q) == false)
	}

	expr := a.Add(b).Add(c)
	assertIsQuorum(expr, map[GenericExpr]bool{Node{Name: "a"}: true})
	assertIsQuorum(expr, map[GenericExpr]bool{Node{Name: "b"}: true})
	assertIsQuorum(expr, map[GenericExpr]bool{Node{Name: "c"}: true})
	assertIsQuorum(expr, map[GenericExpr]bool{Node{Name: "a"}: true, Node{Name: "b"}: true})
	assertIsQuorum(expr, map[GenericExpr]bool{Node{Name: "a"}: true, Node{Name: "c"}: true})
	assertIsQuorum(expr, map[GenericExpr]bool{Node{Name: "b"}: true, Node{Name: "c"}: true})
	assertIsNotQuorum(expr, map[GenericExpr]bool{})
	assertIsNotQuorum(expr, map[GenericExpr]bool{Node{Name: "x"}: true})

	exprAnd := a.Multiply(b).Multiply(c)
	assertIsQuorum(exprAnd, map[GenericExpr]bool{Node{Name: "a"}: true, Node{Name: "b"}: true, Node{Name: "c"}: true})
	assertIsQuorum(exprAnd, map[GenericExpr]bool{Node{Name: "a"}: true, Node{Name: "b"}: true, Node{Name: "c"}: true, Node{Name: "x"}: true})
	assertIsNotQuorum(exprAnd, map[GenericExpr]bool{})
	assertIsNotQuorum(exprAnd, map[GenericExpr]bool{Node{Name: "a"}: true})
	assertIsNotQuorum(exprAnd, map[GenericExpr]bool{Node{Name: "b"}: true})
	assertIsNotQuorum(exprAnd, map[GenericExpr]bool{Node{Name: "c"}: true})
	assertIsNotQuorum(exprAnd, map[GenericExpr]bool{Node{Name: "a"}: true, Node{Name: "b"}: true})
	assertIsNotQuorum(exprAnd, map[GenericExpr]bool{Node{Name: "a"}: true, Node{Name: "c"}: true})
	assertIsNotQuorum(exprAnd, map[GenericExpr]bool{Node{Name: "b"}: true, Node{Name: "c"}: true})
	assertIsNotQuorum(exprAnd, map[GenericExpr]bool{Node{Name: "x"}: true})
	assertIsNotQuorum(exprAnd, map[GenericExpr]bool{Node{Name: "a"}: true, Node{Name: "x"}: true})

	exprp := a.Add(b).Multiply(c.Add(d))
	assertIsQuorum(exprp, map[GenericExpr]bool{Node{Name: "a"}: true, Node{Name: "c"}: true})
	assertIsQuorum(exprp, map[GenericExpr]bool{Node{Name: "a"}: true, Node{Name: "c"}: true})
	assertIsQuorum(exprp, map[GenericExpr]bool{Node{Name: "a"}: true, Node{Name: "d"}: true})
	assertIsQuorum(exprp, map[GenericExpr]bool{Node{Name: "b"}: true, Node{Name: "d"}: true})
	assertIsQuorum(exprp, map[GenericExpr]bool{Node{Name: "a"}: true, Node{Name: "b"}: true, Node{Name: "d"}: true})
	assertIsQuorum(exprp, map[GenericExpr]bool{Node{Name: "b"}: true, Node{Name: "c"}: true, Node{Name: "d"}: true})
	assertIsQuorum(exprp, map[GenericExpr]bool{Node{Name: "b"}: true, Node{Name: "c"}: true, Node{Name: "d"}: true})
	assertIsQuorum(exprp, map[GenericExpr]bool{Node{Name: "a"}: true, Node{Name: "b"}: true, Node{Name: "d"}: true})
	assertIsQuorum(exprp, map[GenericExpr]bool{Node{Name: "a"}: true, Node{Name: "c"}: true, Node{Name: "d"}: true})
	assertIsQuorum(exprp, map[GenericExpr]bool{Node{Name: "a"}: true, Node{Name: "b"}: true, Node{Name: "c"}: true, Node{Name: "d"}: true})
	assertIsNotQuorum(exprp, map[GenericExpr]bool{Node{Name: "a"}: true})
	assertIsNotQuorum(exprp, map[GenericExpr]bool{Node{Name: "b"}: true})
	assertIsNotQuorum(exprp, map[GenericExpr]bool{Node{Name: "c"}: true})
	assertIsNotQuorum(exprp, map[GenericExpr]bool{Node{Name: "d"}: true})
	assertIsNotQuorum(exprp, map[GenericExpr]bool{Node{Name: "a"}: true, Node{Name: "b"}: true})
	assertIsNotQuorum(exprp, map[GenericExpr]bool{Node{Name: "c"}: true, Node{Name: "d"}: true})
	assertIsNotQuorum(exprp, map[GenericExpr]bool{Node{Name: "x"}: true})
}

func TestResilience(t *testing.T) {
	assertResilience := func(expr GenericExpr, n int) {
		assert.Assert(t, expr.Resilience() == n, fmt.Sprintf("Actual: %.10d | Expected  %.10d", expr.Resilience(), n))
	}

	a, b, c, d, e, f := DefNode("a"), DefNode("b"), DefNode("c"), DefNode("d"), DefNode("e"), DefNode("f")

	assertResilience(a, 0)
	assertResilience(a.Add(b), 1)
	assertResilience(a.Add(b).Add(c), 2)
	assertResilience(a.Add(b).Add(c).Add(d), 3)
	assertResilience(a, 0)
	assertResilience(a.Multiply(b), 0)
	assertResilience(a.Multiply(b).Multiply(c), 0)
	assertResilience(a.Multiply(b).Multiply(c).Multiply(d), 0)
	assertResilience(a.Add(b).Multiply(c.Add(d)), 1)
	assertResilience(a.Add(b).Add(c).Multiply(d.Add(e).Add(f)), 2)
	assertResilience((a.Add(b).Add(c)).Multiply(a.Add(e).Add(f)), 2)
	assertResilience(a.Add(a).Add(c).Multiply(d.Add(e).Add(f)), 1)
	assertResilience((a.Add(a).Add(a)).Multiply(d.Add(e).Add(f)), 0)
	assertResilience((a.Multiply(b)).Add(b.Multiply(c)).Add(a.Multiply(d)).Add(a.Multiply(d).Multiply(e)), 1)

	expr, _ := DefChoose(2, []GenericExpr{a, b, c})
	assertResilience(expr, 1)

	expr, _ = DefChoose(2, []GenericExpr{a, b, c, d, e})
	assertResilience(expr, 3)

	expr, _ = DefChoose(3, []GenericExpr{a, b, c, d, e})
	assertResilience(expr, 2)

	expr, _ = DefChoose(4, []GenericExpr{a, b, c, d, e})
	assertResilience(expr, 1)

	expr, _ = DefChoose(4, []GenericExpr{a, b, c, d, e})
	assertResilience(expr, 1)

}

func TestDual(t *testing.T) {

	assertDual := func(x GenericExpr, y GenericExpr) {
		assert.DeepEqual(t, x.Dual(), y)
	}

	a, b, c, d := DefNode("a"), DefNode("b"), DefNode("c"), DefNode("d")

	assertDual(a, a)
	assertDual(a.Add(b), a.Multiply(b))
	assertDual(a.Add(a), a.Multiply(a))
	assertDual((a.Add(b)).Multiply(c.Add(d)), (a.Multiply(b)).Add(c.Multiply(d)))
	assertDual((a.Add(b)).Multiply(a.Add(d)), (a.Multiply(b)).Add(a.Multiply(d)))
	assertDual((a.Add(b)).Multiply(a.Add(a)), (a.Multiply(b)).Add(a.Multiply(a)))
	assertDual((a.Add(a)).Multiply(a.Add(a)), (a.Multiply(a)).Add(a.Multiply(a)))
	assertDual((a.Add(a.Multiply(b))).Add((c.Multiply(d)).Add(a)), (a.Multiply(a.Add(b))).Multiply((c.Add(d)).Multiply(a)))
}

func TestDupFree(t *testing.T) {
	assertDupFree := func(expr GenericExpr) {
		assert.Assert(t, expr.DupFree() == true)
	}

	assertNonDupFree := func(expr GenericExpr) {
		assert.Assert(t, expr.DupFree() == false)
	}

	a, b, c, d, e := DefNode("a"), DefNode("b"), DefNode("c"), DefNode("d"), DefNode("e")

	assertDupFree(a)
	assertDupFree(a.Add(b))
	assertDupFree(a.Multiply(b))
	assertDupFree(a.Multiply(b).Add(c))
	assertDupFree((a.Add(b)).Multiply(c.Add(d.Multiply(e))))
	assertNonDupFree(a.Add(a))
	assertNonDupFree(a.Multiply(a))
	assertNonDupFree(a.Multiply(b.Add(a)))
	assertNonDupFree((a.Add(b)).Multiply(c.Add(d.Multiply(a))))
}
