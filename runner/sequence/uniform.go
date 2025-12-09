package sequence

import "math/rand/v2"

var _ Generator = &uniform{}

type uniform struct {
	maxSequence uint64
}

func (o *uniform) Next() uint64 {
	return rand.Uint64N(o.maxSequence)
}

func newUniform(maxSequence uint64) Generator {
	return &uniform{
		maxSequence: maxSequence,
	}
}
