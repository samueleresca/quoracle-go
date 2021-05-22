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

}