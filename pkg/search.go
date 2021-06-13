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

		go func() {
			chnl <- nodes[0]
			close(chnl)
		}()

		return chnl
	}

	if maxHeight == 1 {

		go func() {
			for k := 1; k < len(nodes)+1; k++ {
				choose, _ := DefChoose(k, nodes)
				chnl <- choose
			}
			close(chnl)
		}()

		return chnl
	}

	go func() {
		for partitioning := range partitionings(nodes) {
			if len(partitioning) == 1 {
				continue
			}

			subiterators := make([][]interface{}, 0)

			for _, p := range partitioning {
				tmp := make([]interface{}, 0)
				for e := range dupFreeExprs(p, maxHeight-1) {
					tmp = append(tmp, e)
				}

				subiterators = append(subiterators, tmp)
			}

			for _, subexprs := range productInterfaces(subiterators...) {

				exprs := make([]GenericExpr, 0)

				for _, se := range subexprs {
					exprs = append(exprs, se.(GenericExpr))
				}

				for k := 1; k < len(subexprs)+1; k++ {
					result, _ := DefChoose(k, exprs)
					chnl <- result
				}
			}
		}

		close(chnl)
	}()

	return chnl
}
