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
