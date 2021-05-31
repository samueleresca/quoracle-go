package pkg

import (
	"gotest.tools/assert"
	"sort"
	"strings"
	"testing"
)

func TestInit(t *testing.T) {

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
	}
	a, b, c := DefNode("a"), DefNode("b"), DefNode("c") ///, DefNode("d")

	qs := DefQuorumSystemWithReads(a.Add(b))
	assertQuorums(qs.Reads, [][]string{{"a"}, {"b"}})
	assertQuorums(qs.Writes, [][]string{{"a", "b"}})

	qs = DefQuorumSystemWithWrites(a.Add(b))
	assertQuorums(qs.Writes, [][]string{{"a"}, {"b"}})
	assertQuorums(qs.Reads, [][]string{{"a", "b"}})

	qs, _ = DefQuorumSystem(a.Add(b), a.Multiply(b).Multiply(c))
	assertQuorums(qs.Reads, [][]string{{"a"}, {"b"}})
	assertQuorums(qs.Writes, [][]string{{"a", "b", "c"}})

	_, err := DefQuorumSystem(a.Add(b), a)

	assert.Error(t, err, "Not all read quorums intersect all write quorums")

}

func TestUniformStrategy(t *testing.T) {

	assertSigma := func(actual []SigmaRecord, expected []SigmaRecord) {
		assert.DeepEqual(t, actual, expected)
	}

	a := DefNode("a")

	sigma, _ := DefQuorumSystemWithReads(a).UniformStrategy(0)

	assertSigma(sigma.SigmaR.Values, []SigmaRecord{{map[GenericExpr]bool{a: true}, 1.0}})
	assertSigma(sigma.SigmaW.Values, []SigmaRecord{{map[GenericExpr]bool{a: true}, 1.0}})

	sigma, _ = DefQuorumSystemWithReads(a.Add(a)).UniformStrategy(0)

	assertSigma(sigma.SigmaR.Values, []SigmaRecord{{map[GenericExpr]bool{a: true}, 1.0}})
	assertSigma(sigma.SigmaW.Values, []SigmaRecord{{map[GenericExpr]bool{a: true}, 1.0}})

	sigma, _ = DefQuorumSystemWithReads(a.Multiply(a)).UniformStrategy(0)

	assertSigma(sigma.SigmaR.Values, []SigmaRecord{{map[GenericExpr]bool{a: true}, 1.0}})
	assertSigma(sigma.SigmaW.Values, []SigmaRecord{{map[GenericExpr]bool{a: true}, 1.0}})

	sigma, _ = DefQuorumSystemWithReads(a.Add(a.Multiply(a))).UniformStrategy(0)

	assertSigma(sigma.SigmaR.Values, []SigmaRecord{{map[GenericExpr]bool{a: true}, 1.0}})
	assertSigma(sigma.SigmaW.Values, []SigmaRecord{{map[GenericExpr]bool{a: true}, 1.0}})
}
