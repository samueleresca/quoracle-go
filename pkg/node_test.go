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
