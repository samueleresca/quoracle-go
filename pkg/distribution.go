package pkg

import "fmt"

type Fraction = float64
type Weight = float64
type Probability = float64

type Distribution interface {
	GetValue() DistributionValues
	IsSingleValue() bool
}

type DistributionValues = map[Fraction]Weight

type QuorumDistribution struct {
	values DistributionValues
}

func (qd QuorumDistribution) GetValue() DistributionValues {
	return qd.values
}

func (qd QuorumDistribution) IsSingleValue() bool {
	return len(qd.values) == 1
}

func canonicalizeRW(readFraction *Distribution, writeFraction *Distribution) (map[Fraction]Probability, error) {

	if *readFraction == nil && *writeFraction == nil {
		return nil, fmt.Errorf("Either readFraction or writeFraction must be given")
	}

	if *readFraction != nil && *writeFraction != nil {
		return nil, fmt.Errorf("Only one of read_fraction or write_fraction can be given")
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
