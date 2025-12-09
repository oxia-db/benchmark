package sequence

import "fmt"

const (
	keyDistributionUniform = "uniform"
	keyDistributionZipf    = "zipf"
	keyDistributionOrder   = "order"
)

type Generator interface {
	Next() uint64
}

func NewGenerator(name string, maxSequence uint64) Generator {
	switch name {
	case keyDistributionUniform:
		return newUniform(maxSequence)
	case keyDistributionZipf:
		return newZipf(maxSequence)
	case keyDistributionOrder:
		return newOrder()
	default:
		panic(fmt.Sprintf("sequence generator '%s' is not supported", name))
	}
}
