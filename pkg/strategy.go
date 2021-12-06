package pkg

import (
	wr "github.com/mroth/weightedrand"
	"math/rand"
	"time"
)

// OptimizeType describes an optimization type
type OptimizeType string

const (
	Load    OptimizeType = "Load"
	Network OptimizeType = "Network"
	Latency OptimizeType = "Latency"
)

// StrategyOptions describes the quorum system strategy options.
type StrategyOptions struct {
	// Optimize defines the target optimization.
	Optimize OptimizeType
	// LoadLimit defines the limit on the load limit.
	LoadLimit *float64
	// NetworkLimit defines the limit on the network limit.
	NetworkLimit *float64
	// LatencyLimit defines the limit on the latency.
	LatencyLimit *float64
	// ReadFraction defines the workflow distribution for the read operations.
	ReadFraction Distribution
	// WriteFraction defines the workflow distribution for the write operations.
	WriteFraction Distribution
	// F r ∈ R is F-resilient for some integer f if despite removing
	// any f nodes from r, r is still a read quorum
	F uint
}

//Strategy defines a strategy related to a QuorumSystem.
type Strategy struct {
	Qs                     QuorumSystem
	SigmaR                 Sigma
	SigmaW                 Sigma
	nodeToReadProbability  map[Node]Probability
	nodeToWriteProbability map[Node]Probability
}

// Sigma defines the probabilities of a specific Strategy. Each Expr (quorum) has a probability of being choose associated.
type Sigma struct {
	Values []SigmaRecord
}

// SigmaRecord defines as ExprSet that represents a quorum and the probability of being chosen.
type SigmaRecord struct {
	Quorum      ExprSet
	Probability Probability
}

// NewStrategy returns a new Strategy given a QuorumSystem and the Sigma related to the read/writes.
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

	newStrategy.nodeToWriteProbability = xWriteProbability
	newStrategy.nodeToReadProbability = xReadProbability

	return newStrategy
}

// GetReadQuorum returns a ExprSet representing a quorum of the strategy.
// The method return the quorum based on its probability.
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

// GetWriteQuorum returns a ExprSet representing a quorum of the strategy.
// The method return the quorum based on its probability.
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

// Load calculates and returns the load of the strategy given a read and write distribution.
func (s Strategy) Load(rf *Distribution, wf *Distribution) (float64, error) {
	d, err := canonicalizeReadsWrites(rf, wf)
	if err != nil {
		return 0, err
	}

	sum := 0.0

	for fr, p := range d {
		sum += p * s.getMaxLoad(fr)
	}
	return sum, nil
}

// Capacity calculates and returns the capacity of the strategy given a read and write distribution.
func (s Strategy) Capacity(rf *Distribution, wf *Distribution) (float64, error) {
	d, err := canonicalizeReadsWrites(rf, wf)
	if err != nil {
		return -1, err
	}

	sum := 0.0

	for fr, p := range d {
		sum += p * 1.0 / s.getMaxLoad(fr)
	}
	return sum, nil
}

// NetworkLoad calculates and returns the network load of the strategy given a read and write Distribution.
func (s Strategy) NetworkLoad(rf *Distribution, wf *Distribution) (float64, error) {
	d, err := canonicalizeReadsWrites(rf, wf)
	if err != nil {
		return -1, err
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
	return total, nil
}

// Latency calculates and returns the latency of the strategy given a read and write Distribution.
func (s Strategy) Latency(rf *Distribution, wf *Distribution) (float64, error) {
	d, err := canonicalizeReadsWrites(rf, wf)
	if err != nil {
		return -1, err
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
			return -1, err
		}

		reads += float64(v) * rq.Probability
	}

	writes := 0.0

	for _, wq := range s.SigmaW.Values {
		nodes := make([]Node, 0)

		for n := range wq.Quorum {
			nodes = append(nodes, s.Qs.GetNodeByName(n.String()))
		}

		v, err := s.Qs.writeQuorumLatency(nodes)

		if err != nil {
			return -1, err
		}
		writes += float64(v) * wq.Probability
	}

	total := frsum*reads + (1-frsum)*writes
	return total, nil
}

// NodeLoad returns the load of a specific Node given a read and write Distribution.
func (s Strategy) NodeLoad(node Node, rf *Distribution, wf *Distribution) (float64, error) {
	d, err := canonicalizeReadsWrites(rf, wf)
	if err != nil {
		return -1, err
	}

	sum := 0.0

	for fr, p := range d {
		sum += p * s.getNodeLoad(node, fr)
	}
	return sum, nil
}

// NodeUtilization returns the utilization of a specific Node given a read and write Distribution.
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

// NodeThroughput returns the throughput of a specific Node given a read and write Distribution.
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

func (s Strategy) String() string {
	return "TODO"
}

// getMaxLoad returns the max load of the strategy for a specific fraction.
func (s Strategy) getMaxLoad(fr float64) float64 {
	max := 0.0

	for n := range s.Qs.GetNodes() {
		if s.getNodeLoad(n, fr) > max {
			max = s.getNodeLoad(n, fr)
		}
	}

	return max
}

// getNodeLoad returns the load of a node for a given probability.
func (s Strategy) getNodeLoad(node Node, fr float64) float64 {
	fw := 1 - fr
	return fr*s.nodeToReadProbability[node]/float64(*node.ReadCapacity) +
		fw*s.nodeToWriteProbability[node]/float64(*node.WriteCapacity)
}

func (s Strategy) nodeUtilization(node Node, fr float64) float64 {
	return s.getNodeLoad(node, fr) / s.getMaxLoad(fr)
}

func (s Strategy) nodeThroughput(node Node, fr float64) float64 {
	capacity := 1 / s.getMaxLoad(fr)
	fw := 1 - fr

	return capacity * (fr*s.nodeToReadProbability[node] + fw*s.nodeToWriteProbability[node])
}

func initStrategyOptions(initOptions StrategyOptions) func(options *StrategyOptions) error {
	init := func(options *StrategyOptions) error {
		options.Optimize = initOptions.Optimize
		options.LatencyLimit = initOptions.LatencyLimit
		options.NetworkLimit = initOptions.NetworkLimit
		options.LoadLimit = initOptions.LoadLimit
		options.F = initOptions.F
		options.ReadFraction = initOptions.ReadFraction
		options.WriteFraction = initOptions.WriteFraction

		return nil
	}
	return init
}
