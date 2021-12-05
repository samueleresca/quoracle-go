package pkg

import "fmt"

type Fraction = float64
type Weight = float64
type Probability = float64

// Distribution describes a distribution of values.
type Distribution interface {
	GetValue() DistributionValues
	IsSingleValue() bool
}

// DistributionValues describe a set of fraction over weight.
type DistributionValues = map[Fraction]Weight

// QuorumDistribution describes a list of DistributionValues
type QuorumDistribution struct {
	values DistributionValues
}

// GetValue gets the list of DistributionValues.
func (qd QuorumDistribution) GetValue() DistributionValues {
	return qd.values
}

// IsSingleValue checks if the list of DistributionValues has only 1 element.
func (qd QuorumDistribution) IsSingleValue() bool {
	return len(qd.values) == 1
}

// canonicalizeReadsWrites canonicalizes the read Distribution and the write Distribution.
func canonicalizeReadsWrites(readFraction *Distribution, writeFraction *Distribution) (map[Fraction]Probability, error) {

	if *readFraction == nil && *writeFraction == nil {
		return nil, fmt.Errorf("either readFraction or writeFraction must be given")
	}

	if *readFraction != nil && *writeFraction != nil {
		return nil, fmt.Errorf("only one of read_fraction or write_fraction can be given")
	}

	if *readFraction != nil {
		return canonicalize(readFraction)
	}

	if *writeFraction != nil {
		cDist, err := canonicalize(writeFraction)

		if err != nil {
			return nil, err
		}

		r := make(map[Fraction]Probability)

		for f, p := range cDist {
			r[1-f] = p
		}

		return r, nil
	}

	return nil, fmt.Errorf("writeFraction not specified")
}

//canonicalize checks and proceeds by converting the distribution in a standard distribution.
// - no negative weights.
// - no zero weight.
func canonicalize(d *Distribution) (map[Fraction]Probability, error) {
	if d == nil || len((*d).GetValue()) == 0 {
		return nil, fmt.Errorf("distribution cannot be nil")
	}

	var totalWeight Weight = 0

	for _, w := range (*d).GetValue() {
		if w < 0 {
			return nil, fmt.Errorf("distribution cannot have negative weights")
		}
		totalWeight += w
	}

	if totalWeight == 0 {
		return nil, fmt.Errorf("distribution cannot have zero weight")
	}

	result := make(map[Fraction]Probability)

	for f, w := range (*d).GetValue() {
		if w > 0 {
			result[f] = w / totalWeight
		}
	}

	return result, nil
}
