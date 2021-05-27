package pkg

import "fmt"

type OptimizeType string

const(
	Load OptimizeType = "Load"
	Network OptimizeType = "Network"
	Latency OptimizeType= "Latency"
)

type QuorumSystem struct {
	Reads GenericExpr
	Writes GenericExpr
	XtoNode map[string]Node
}

type StrategyOptions struct {
	Optimize OptimizeType
	LoadLimit *float64
	NetworkLimit *float64
	LatencyLimit *float64
	ReadFraction Distribution
	WriteFraction Distribution
	F *int
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

func (qs QuorumSystem) Strategy (opts ...func(options *StrategyOptions) error) (*Strategy, error) {

	sb := &StrategyOptions{}
	// ... (write initializations with default values)...
	for _, op := range opts{
		err := op(sb)
		if err != nil {
			return nil, err
		}
	}

	if sb.Optimize == Load && sb.LoadLimit != nil {
		return nil, fmt.Errorf("a load limit cannot be set when optimizing for load")
	}

	if sb.Optimize == Network && sb.NetworkLimit != nil {
		return nil, fmt.Errorf("a network limit cannot be set when optimizing for network")
	}

	if sb.Optimize == Latency && sb.LatencyLimit != nil {
		return nil, fmt.Errorf("a latency limit cannot be set when optimizing for latency")
	}

	if sb.F != nil && *sb.F < 0 {
		return nil, fmt.Errorf("f must be >= 0")
	}

}

type Strategy struct{

}