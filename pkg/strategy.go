package pkg

import (
	wr "github.com/mroth/weightedrand"
	"math/rand"
	"sort"
	"time"
)

//Strategy
type Strategy struct {
	Qs                QuorumSystem
	SigmaR            Sigma
	SigmaW            Sigma
	XReadProbability  map[Node]float64
	XWriteProbability map[Node]float64
}

type SigmaRecord struct {
	Quorum      ExprSet
	Probability Probability
}
type Sigma struct {
	Values []SigmaRecord
}

func NewStrategy(quorumSystem QuorumSystem, sigmaR Sigma, sigmaW Sigma) Strategy {
	newStrategy := Strategy{SigmaR: sigmaR, SigmaW: sigmaW, Qs: quorumSystem}

	xReadProbability := make(map[Node]float64)
	for _, sr := range sigmaR.Values {
		for q := range sr.Quorum {
			xReadProbability[q.(Node)] += sr.Probability
		}

	}

	xWriteProbability := make(map[Node]float64)
	for _, sr := range sigmaW.Values {
		for q := range sr.Quorum {
			xWriteProbability[q.(Node)] += sr.Probability
		}
	}

	newStrategy.XWriteProbability = xWriteProbability
	newStrategy.XReadProbability = xReadProbability

	return newStrategy
}

func (s Strategy) String() string {
	return "TODO"
}

func (s Strategy) GetReadQuorum() ExprSet {

	rand.Seed(time.Now().UTC().UnixNano()) // always seed random!

	criteria := make([]wr.Choice, 0)

	weightSum := 0.0
	for _, w := range s.SigmaR.Values {
		weightSum += w.Probability
	}

	for _, sigmaRecord := range s.SigmaR.Values {
		criteria = append(criteria, wr.Choice{Item: sigmaRecord.Quorum, Weight: uint(sigmaRecord.Probability * 10)})
	}

	chooser, _ := wr.NewChooser(criteria...)
	result := chooser.Pick().(ExprSet)

	return result
}

func (s Strategy) GetWriteQuorum() ExprSet {

	rand.Seed(time.Now().UTC().UnixNano()) // always seed random!

	criteria := make([]wr.Choice, 0)

	weightSum := 0.0
	for _, w := range s.SigmaW.Values {
		weightSum += w.Probability
	}

	for _, sigmaRecord := range s.SigmaW.Values {
		criteria = append(criteria, wr.Choice{Item: sigmaRecord.Quorum, Weight: uint(sigmaRecord.Probability * 10)})
	}

	chooser, _ := wr.NewChooser(criteria...)
	result := chooser.Pick().(ExprSet)

	return result
}

func (s Strategy) Load(rf *Distribution, wf *Distribution) (*float64, error) {
	d, err := canonicalizeReadsWrites(rf, wf)
	if err != nil {
		return nil, err
	}

	sum := 0.0

	for fr, p := range d {
		sum += p * s.maxLoad(fr)
	}
	return &sum, nil
}

func (s Strategy) Capacity(rf *Distribution, wf *Distribution) (*float64, error) {
	d, err := canonicalizeReadsWrites(rf, wf)
	if err != nil {
		return nil, err
	}

	sum := 0.0

	for fr, p := range d {
		sum += p * 1.0 / s.maxLoad(fr)
	}
	return &sum, nil
}

func (s Strategy) NetworkLoad(rf *Distribution, wf *Distribution) (*float64, error) {
	d, err := canonicalizeReadsWrites(rf, wf)
	if err != nil {
		return nil, err
	}

	frsum := 0.0

	for fr, p := range d {
		frsum += p * fr
	}

	reads := 0.0
	for _, sigma := range s.SigmaR.Values {
		reads += frsum * float64(len(sigma.Quorum)) * sigma.Probability
	}

	writes := 0.0
	for _, sigma := range s.SigmaW.Values {
		writes += (1 - frsum) * float64(len(sigma.Quorum)) * sigma.Probability
	}

	total := reads + writes
	return &total, nil
}

func (s Strategy) Latency(rf *Distribution, wf *Distribution) (*float64, error) {
	d, err := canonicalizeReadsWrites(rf, wf)
	if err != nil {
		return nil, err
	}

	frsum := 0.0

	for fr, p := range d {
		frsum += p * fr
	}

	reads := 0.0

	for _, rq := range s.SigmaR.Values {
		nodes := make([]Node, 0)

		for n := range rq.Quorum {
			nodes = append(nodes, s.Qs.GetNodeByName(n.String()))
		}

		v, err := s.Qs.readQuorumLatency(nodes)

		if err != nil {
			return nil, err
		}

		reads += float64(*v) * rq.Probability
	}

	writes := 0.0

	for _, wq := range s.SigmaW.Values {
		nodes := make([]Node, 0)

		for n := range wq.Quorum {
			nodes = append(nodes, s.Qs.GetNodeByName(n.String()))
		}

		v, err := s.Qs.writeQuorumLatency(nodes)

		if err != nil {
			return nil, err
		}
		writes += float64(*v) * wq.Probability
	}

	total := frsum*reads + (1-frsum)*writes
	return &total, nil
}

func (s Strategy) NodeLoad(node Node, rf *Distribution, wf *Distribution) (*float64, error) {
	d, err := canonicalizeReadsWrites(rf, wf)
	if err != nil {
		return nil, err
	}

	sum := 0.0

	for fr, p := range d {
		sum += p * s.nodeLoad(node, fr)
	}
	return &sum, nil
}

func (s Strategy) NodeUtilization(node Node, rf *Distribution, wf *Distribution) (*float64, error) {
	d, err := canonicalizeReadsWrites(rf, wf)
	if err != nil {
		return nil, err
	}

	sum := 0.0

	for fr, p := range d {
		sum += p * s.nodeUtilization(node, fr)
	}
	return &sum, nil
}

func (s Strategy) NodeThroughput(node Node, rf *Distribution, wf *Distribution) (*float64, error) {
	d, err := canonicalizeReadsWrites(rf, wf)
	if err != nil {
		return nil, err
	}

	sum := 0.0

	for fr, p := range d {
		sum += p * s.nodeThroughput(node, fr)
	}
	return &sum, nil
}

func (s Strategy) maxLoad(fr float64) float64 {
	max := 0.0

	for n := range s.Qs.GetNodes() {
		if s.nodeLoad(n, fr) > max {
			max = s.nodeLoad(n, fr)
		}
	}

	return max
}

func (s Strategy) nodeLoad(node Node, fr float64) float64 {
	fw := 1 - fr
	return fr*s.XReadProbability[node]/float64(*node.ReadCapacity) +
		fw*s.XWriteProbability[node]/float64(*node.WriteCapacity)
}

func (s Strategy) nodeUtilization(node Node, fr float64) float64 {
	return s.nodeLoad(node, fr) / s.maxLoad(fr)
}

func (s Strategy) nodeThroughput(node Node, fr float64) float64 {
	capacity := 1 / s.maxLoad(fr)
	fw := 1 - fr

	return capacity * (fr*s.XReadProbability[node] + fw*s.XWriteProbability[node])
}

// Sorter
type nodeSorter struct {
	nodes []Node
	by    func(p1, p2 *Node) bool // Closure used in the Less method.
}

// Len is part of sort.Interface.
func (ns *nodeSorter) Len() int {
	return len(ns.nodes)
}

// Swap is part of sort.Interface.
func (ns *nodeSorter) Swap(i, j int) {
	ns.nodes[i], ns.nodes[j] = ns.nodes[j], ns.nodes[i]
}

// Less is part of sort.Interface. It is implemented by calling the "by" closure in the sorter.
func (ns *nodeSorter) Less(i, j int) bool {
	return ns.by(&ns.nodes[i], &ns.nodes[j])
}

type By func(p1, p2 *Node) bool

// Sort is a method on the function type, By, that sorts the argument slice according to the function.
func (by By) Sort(nodes []Node) {
	ps := &nodeSorter{
		nodes: nodes,
		by:    by, // The Sort method's receiver is the function (closure) that defines the sort order.
	}
	sort.Sort(ps)
}
