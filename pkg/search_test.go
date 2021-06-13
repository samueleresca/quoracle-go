package pkg

import (
	"fmt"
	"gotest.tools/assert"
	"reflect"
	"testing"
)

func TestPartitions(t *testing.T) {

	node1, node2, node3, node4 := DefNode("1"), DefNode("2"), DefNode("3"), DefNode("4")

	for r := range partitionings([]GenericExpr{}) {
		assert.Assert(t, reflect.DeepEqual(r, [][]GenericExpr{}))
	}

	for r := range partitionings([]GenericExpr{node1}) {
		assert.Assert(t, reflect.DeepEqual(r, [][]GenericExpr{{node1}}))
	}

	result := partitionings([]GenericExpr{node1, node2})

	result1 := <-result
	result2 := <-result

	assert.Assert(t, reflect.DeepEqual(result1, [][]GenericExpr{{node1}, {node2}}) == true)
	assert.Assert(t, reflect.DeepEqual(result2, [][]GenericExpr{{node1, node2}}) == true)

	expected := map[string]bool{
		"[[1] [2] [3]]": true,
		"[[1 2] [3]]":   true,
		"[[2] [1 3]]":   true,
		"[[1] [2 3]]":   true,
		"[[1 2 3]]":     true,
	}

	index := 0
	for actual := range partitionings([]GenericExpr{node1, node2, node3}) {
		_, ok := expected[fmt.Sprint(actual)]
		assert.Assert(t, ok == true, actual)
		index++
	}

	expected = map[string]bool{
		"[[1] [2] [3] [4]]": true,
		"[[1 2] [3] [4]]":   true,
		"[[2] [1 3] [4]]":   true,
		"[[2] [3] [1 4]]":   true,
		"[[1] [2 3] [4]]":   true,
		"[[1] [3] [2 4]]":   true,
		"[[1] [2] [3 4]]":   true,
		"[[1 2] [3 4]]":     true,
		"[[1 3] [2 4]]":     true,
		"[[2 3] [1 4]]":     true,
		"[[1] [2 3 4]]":     true,
		"[[2] [1 3 4]]":     true,
		"[[3] [1 2 4]]":     true,
		"[[1 2 3] [4]]":     true,
		"[[1 2 3 4]]":       true,
	}

	index = 0
	for actual := range partitionings([]GenericExpr{node1, node2, node3, node4}) {
		_, ok := expected[fmt.Sprint(actual)]
		assert.Assert(t, ok == true, actual)
		index++
	}
}
