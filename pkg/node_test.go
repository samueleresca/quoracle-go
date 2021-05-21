package pkg

import (
	"gotest.tools/assert"
	"testing"
)
func assertQuorums(t *testing.T, e GenericExpr, xs []map[string]bool ){
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
func TestNode(t *testing.T ){
	a, b, c := DefNode("a"), DefNode("b"), DefNode("c")
	assert.Assert(t, a.String() == "a")
	assert.Assert(t, b.String() == "b")
	assert.Assert(t, c.String() == "c")
}

func TestQuorums(t *testing.T){
	a, b, c := DefNode("a"), DefNode("b"), DefNode("c")
	assert.Assert(t, a.Add(b).Add(c).String() == "(a + b + c)" )
	assert.Assert(t, a.Multiply(b).Multiply(c).String() == "(a * b * c)" )
	assert.Assert(t, a.Add(b.Multiply(c)).String() == "(a + (b * c))")

	assertQuorums(t, a.Multiply(b).Multiply(c), []map[string]bool{{"a": true, "b": true, "c": true}})
	assertQuorums(t, a.Add(b).Add(c) , []map[string]bool{{"a": true },{"b": true},{ "c": true}})

	assertQuorums(t, a.Add(b.Multiply(c)) , []map[string]bool{{"a": true },{"b": true, "c": true}})
	//assertQuorums(t, a.Add(a).Add(a) , []map[string]bool{{"a": true }})
	//assertQuorums(t, a.Multiply(a).Multiply(a) , []map[string]bool{{"a": true }})
	assertQuorums(t, a.Multiply(a.Add(b)) , []map[string]bool{{"a": true },{"a": true, "b": true,}})
}
