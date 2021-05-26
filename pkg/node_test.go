package pkg

import (
	"gotest.tools/assert"
	"testing"
)

func TestNode(t *testing.T ){
	a, b, c := DefNode("a"), DefNode("b"), DefNode("c")
	assert.Assert(t, a.String() == "a")
	assert.Assert(t, b.String() == "b")
	assert.Assert(t, c.String() == "c")
}

func TestQuorums(t *testing.T){
	assertQuorums := func(t *testing.T, e GenericExpr, xs []map[string]bool ){
		nodes := make([]map[string]bool,0)

		for q:= range e.Quorums(){
			tmp := make(map[string]bool)

			for n:= range q {
				tmp[n.String()] = true
			}
			nodes = append(nodes,tmp)
		}
		assert.DeepEqual(t, nodes, xs)
	}

	a, b, c := DefNode("a"), DefNode("b"), DefNode("c")
	assert.Assert(t, a.Add(b).Add(c).String() == "(a + b + c)" )
	assert.Assert(t, a.Multiply(b).Multiply(c).String() == "(a * b * c)" )
	assert.Assert(t, a.Add(b.Multiply(c)).String() == "(a + (b * c))")

	assertQuorums(t, a.Multiply(b).Multiply(c), []map[string]bool{{"a": true, "b": true, "c": true}})
	assertQuorums(t, a.Add(b).Add(c) , []map[string]bool{{"a": true },{"b": true},{ "c": true}})

	assertQuorums(t, a.Add(b.Multiply(c)) , []map[string]bool{{"a": true },{"b": true, "c": true}})
	assertQuorums(t, a.Multiply(a).Multiply(a) , []map[string]bool{{"a": true }})
	assertQuorums(t, a.Multiply(a.Add(b)) , []map[string]bool{{"a": true },{"a": true, "b": true,}})
	assertQuorums(t, a.Multiply(a.Add(b)), []map[string]bool{{"a": true },{"a": true, "b": true,}} )
	assertQuorums(t, a.Add(b).Multiply(a.Add(c)), []map[string]bool{{"a": true },{"a": true, "c": true,},{"a": true, "b": true,},{"b": true, "c": true,}})
}


func TestIsQuorum(t *testing.T){

	a, b, c, d := DefNode("a"), DefNode("b"), DefNode("c"), DefNode("d")

	assertQuorum := func(expr GenericExpr, q map[GenericExpr]bool){
		assert.Assert(t, expr.IsQuorum(q) == true)
	}

	assertNonQuorum := func(expr GenericExpr, q map[GenericExpr]bool){
		assert.Assert(t, expr.IsQuorum(q) == false)
	}

	expr := a.Add(b).Add(c)
	assertQuorum(expr, map[GenericExpr]bool{Node{ Name: "a"}: true})
	assertQuorum(expr,  map[GenericExpr]bool{Node{Name: "b"}: true})
	assertQuorum(expr, map[GenericExpr]bool{Node{Name: "c"}: true})
	assertQuorum(expr,  map[GenericExpr]bool{Node{Name: "a"}: true, Node{Name: "b"}: true})
	assertQuorum(expr, map[GenericExpr]bool{Node{Name: "a"}: true, Node{Name: "c"}: true})
	assertQuorum(expr,  map[GenericExpr]bool{Node{Name: "b"}: true, Node{Name: "c"}: true})
	assertNonQuorum(expr, map[GenericExpr]bool{})
	assertNonQuorum(expr,  map[GenericExpr]bool{Node{Name: "x"}: true})

	exprAnd := a.Multiply(b).Multiply(c)
	assertQuorum(exprAnd, map[GenericExpr]bool{Node{ Name: "a"}: true, Node{ Name: "b"}: true, Node{ Name: "c"}: true})
	assertQuorum(exprAnd,  map[GenericExpr]bool{Node{ Name: "a"}: true, Node{ Name: "b"}: true, Node{ Name: "c"}: true, Node{ Name: "x"}: true})
	assertNonQuorum(exprAnd, map[GenericExpr]bool{})
	assertNonQuorum(exprAnd, map[GenericExpr]bool{Node{ Name: "a"}: true})
	assertNonQuorum(exprAnd,  map[GenericExpr]bool{Node{Name: "b"}: true})
	assertNonQuorum(exprAnd, map[GenericExpr]bool{Node{Name: "c"}: true})
	assertNonQuorum(exprAnd,  map[GenericExpr]bool{Node{Name: "a"}: true, Node{Name: "b"}: true})
	assertNonQuorum(exprAnd, map[GenericExpr]bool{Node{Name: "a"}: true, Node{Name: "c"}: true})
	assertNonQuorum(exprAnd,  map[GenericExpr]bool{Node{Name: "b"}: true, Node{Name: "c"}: true})
	assertNonQuorum(exprAnd, map[GenericExpr]bool{Node{Name: "x"}: true})
	assertNonQuorum(exprAnd,  map[GenericExpr]bool{Node{Name: "a"}: true, Node{Name: "x"}: true})

	exprp := a.Add(b).Multiply(c.Add(d))
	assertQuorum(exprp, map[GenericExpr]bool{Node{ Name: "a"}: true, Node{ Name: "c"}: true})
	assertQuorum(exprp,  map[GenericExpr]bool{Node{ Name: "a"}: true, Node{ Name: "c"}: true})
	assertQuorum(exprp, map[GenericExpr]bool{Node{ Name: "a"}: true, Node{ Name: "d"}: true})
	assertQuorum(exprp, map[GenericExpr]bool{Node{ Name: "b"}: true, Node{ Name: "d"}: true})
	assertQuorum(exprp,  map[GenericExpr]bool{Node{ Name: "a"}: true, Node{ Name: "b"}: true, Node{ Name: "d"}: true})
	assertQuorum(exprp,  map[GenericExpr]bool{Node{ Name: "b"}: true, Node{ Name: "c"}: true, Node{ Name: "d"}: true})
	assertQuorum(exprp,  map[GenericExpr]bool{Node{ Name: "b"}: true, Node{ Name: "c"}: true, Node{ Name: "d"}: true})
	assertQuorum(exprp,  map[GenericExpr]bool{Node{ Name: "a"}: true, Node{ Name: "b"}: true, Node{ Name: "d"}: true})
	assertQuorum(exprp,  map[GenericExpr]bool{Node{ Name: "a"}: true, Node{ Name: "c"}: true, Node{ Name: "d"}: true})
	assertQuorum(exprp,  map[GenericExpr]bool{Node{ Name: "a"}: true, Node{ Name: "b"}: true, Node{ Name: "c"}: true, Node{ Name: "d"}: true})
	assertNonQuorum(exprp, map[GenericExpr]bool{Node{ Name: "a"}: true})
	assertNonQuorum(exprp,  map[GenericExpr]bool{Node{Name: "b"}: true})
	assertNonQuorum(exprp, map[GenericExpr]bool{Node{Name: "c"}: true})
	assertNonQuorum(exprp, map[GenericExpr]bool{Node{Name: "d"}: true})
	assertNonQuorum(exprp,  map[GenericExpr]bool{Node{Name: "a"}: true, Node{Name: "b"}: true})
	assertNonQuorum(exprp,  map[GenericExpr]bool{Node{Name: "c"}: true, Node{Name: "d"}: true})
	assertNonQuorum(exprp, map[GenericExpr]bool{Node{Name: "x"}: true})
}


func TestResilience(t *testing.T){
	assertResilience := func(expr GenericExpr, n int){
		assert.Assert(t, expr.Resilience() == n)
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

}

func TestDual(t *testing.T){
	assertDual := func(x GenericExpr, y GenericExpr ){
		assert.DeepEqual(t, x.Dual(), y)
	}

	a, b, c, d := DefNode("a"), DefNode("b"), DefNode("c"), DefNode("d")

	assertDual(a, a)
	assertDual(a.Add(b), a.Multiply(b))
	assertDual(a.Add(a), a.Multiply(a))
	assertDual((a.Add(b)).Multiply(c.Add(d)), (a.Multiply(b)).Add(c.Multiply(d)))
	assertDual((a.Add(b)).Multiply(a.Add(d)), (a.Multiply(b)).Add(a.Multiply(d)))
	assertDual((a.Add(b)).Multiply(a.Add(a)), (a.Multiply(b)).Add(a.Multiply(a)))
	assertDual((a.Add(a)).Multiply(a.Add(a)),  (a.Multiply(a)).Add(a.Multiply(a)))
	assertDual((a.Add(a.Multiply(b))).Add((c.Multiply(d)).Add(a)), (a.Multiply(a.Add(b))).Multiply((c.Add(d)).Multiply(a)))
}