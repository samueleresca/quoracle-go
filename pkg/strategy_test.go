package pkg

import (
	"gotest.tools/assert"
	"testing"
)

func TestGetQuorum(t *testing.T) {
	a, b, c, d, e := DefNode("a"), DefNode("b"), DefNode("c"), DefNode("d"), DefNode("e")

	choose1, _ := DefChoose(2, []GenericExpr{a, b, c})
	choose2, _ := DefChoose(2, []GenericExpr{a, b, c, d, e})

	exprs := []GenericExpr{a,
		a.Add(b),
		a.Add(b).Add(c),
		choose1,
		choose2,
		(a.Add(b)).Multiply(c.Add(d)),
		(a.Multiply(b)).Add(c.Multiply(d)),
	}

	for _, expr := range exprs {
		qs := DefQuorumSystemWithReads(expr)
		sigma, _ := qs.UniformStrategy(0)

		for i := 0; i < 10; i++ {
			assert.Assert(t, qs.IsReadQuorum(sigma.GetReadQuorum()))
			assert.Assert(t, qs.IsWriteQuorum(sigma.GetWriteQuorum()))
		}

	}
}
