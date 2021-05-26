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

func DefQuorumSystemWithWrites(writes GenericExpr) QuorumSystem {
	return QuorumSystem{}
}


func (qs QuorumSystem) String() string {
	return ""
}

func (qs QuorumSystem) ReadQuorums() chan map[GenericExpr] bool{
	return qs.Reads.Quorums()
}

func (qs QuorumSystem) WriteQuorums() chan map[GenericExpr] bool{
	return qs.Writes.Quorums()
}

func (qs QuorumSystem) IsReadQuorum(xs map[GenericExpr]bool) bool{
	return qs.Reads.IsQuorum(xs)
}

func (qs QuorumSystem) IsWriteQuorum(xs map[GenericExpr]bool) bool{
	return qs.Writes.IsQuorum(xs)
}

func (qs QuorumSystem) Node(x string) Node{
	return qs.XtoNode[x]
}

func (qs QuorumSystem) Nodes() map[Node]bool {
	r := make(map[Node]bool,0)

	for n := range qs.Reads.Nodes() {
		r[n] = true
	}

	for n := range qs.Writes.Nodes() {
		r[n] = true
	}

	return r
}

func (qs QuorumSystem) Elements() map[string]bool{
	r := make(map[string]bool, 0)

	for n := range qs.Nodes() {
		r[n.String()] = true
	}

	return r
}

func (qs QuorumSystem) Resilience() int{
	rr := qs.ReadResilience()
	ww := qs.WriteResilience()

	if rr < ww {
		return rr
	}

	return ww
}

func (qs QuorumSystem) ReadResilience() int {
	return qs.Reads.Resilience()
}

func (qs QuorumSystem) WriteResilience() int {
	return qs.Writes.Resilience()
}

func (qs QuorumSystem) DupFree() bool {
	return qs.Reads.DupFree() && qs.Writes.DupFree()
}
