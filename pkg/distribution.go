package pkg

import "fmt"

type Fraction = float64
type Weight = float64
type Probability = float64

type Distribution interface {
	GetValue() map[Fraction]Weight
}


func canonicalizeRW(readFraction *Distribution, writeFraction *Distribution) (map[Fraction]Probability, error){

	if readFraction == nil && writeFraction == nil {
		return nil, fmt.Errorf("Either readFraction or writeFraction must be given")
	}

	if readFraction != nil && writeFraction != nil {
		return nil, fmt.Errorf("Only one of read_fraction or write_fraction can be given")
	}

	if (readFraction != nil){
		return canonicalize(readFraction)
	}

	if(writeFraction != nil){
		return map[Fraction]Probability{}
	}

	return nil, fmt.Errorf("writeFraction not specified")
}

func canonicalize(fraction *Distribution) (map[Fraction]Probability, error) {

}