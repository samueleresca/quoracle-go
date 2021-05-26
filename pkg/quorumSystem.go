package pkg


type QuorumSystem struct {
	Reads GenericExpr
	Writes GenericExpr
	XtoNode map[string]Node
}

func DefQuorumSystem(reads GenericExpr, writes GenericExpr) QuorumSystem {
	return QuorumSystem{}
}

func DefQuorumSystemWithReads(reads GenericExpr) QuorumSystem {
	return QuorumSystem{}
}

func DefQuorumSystemWithWrites(reads GenericExpr) QuorumSystem {
	return QuorumSystem{}
}
