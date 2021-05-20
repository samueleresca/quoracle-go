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
	a, b, c := DefNode("a"), DefNode("b"), DefNode("c")
	assert.Assert(t, a.Add(b).Add(c).String() == "(a + b + c)" )
	assert.Assert(t, a.Multiply(b).Multiply(c).String() == "(a * b * c)" )
}
