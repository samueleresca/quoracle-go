package pkg

func partitionings(xs []GenericExpr) chan [][]GenericExpr {

	return partitioningsHelper(xs)

}

func partitioningsHelper(xs []GenericExpr) chan [][]GenericExpr {
	chnl := make(chan [][]GenericExpr)
	if len(xs) == 0 {
		go func() {
			chnl <- [][]GenericExpr{}
			close(chnl)
		}()
		return chnl
	}

	x := xs[0]
	rest := xs[1:]

	go func() {
		for partition := range partitioningsHelper(rest) {
			newPartition := partition
			newPartition = append([][]GenericExpr{{x}}, newPartition...)

			chnl <- newPartition

			for i := 0; i < len(partition); i++ {
				result := make([][]GenericExpr, 0)
				result = append(result, partition[:i]...)
				result = append(result, append([]GenericExpr{x}, partition[i]...))

				chnl <- append(result, partition[i+1:]...)

			}
		}
		close(chnl)
	}()
	return chnl
}

func dupFreeExprs(nodes []GenericExpr, maxHeight int) chan GenericExpr {
	chnl := make(chan GenericExpr, 0)

	if len(nodes) == 1 {
		chnl <- nodes[0]

		close(chnl)
		return chnl
	}

	if maxHeight == 1 {
		for k := 1; k < len(nodes)+1; k++ {
			choose, _ := DefChoose(k, nodes)
			chnl <- choose
		}
		close(chnl)
		return chnl
	}

	for partitioning := range partitionings(nodes) {
		if len(partitioning) == 1 {
			continue
		}

		subiterators := make([]GenericExpr, 0)

		for _, p := range partitioning {
			subiterators = append(subiterators, <-dupFreeExprs(p, maxHeight-1))
		}

		for _, subexprs := range product(subiterators) {
			for k := 1; k < len(subexprs)+1; k++ {
				result, _ := DefChoose(k, subexprs)
				chnl <- result
			}
		}
	}

	close(chnl)
	return chnl
}
