package sequence

import (
	"sync/atomic"
)

var _ Generator = &order{}

type order struct {
	maxSequence uint64
	sequence    atomic.Uint64
}

func (o *order) Next() uint64 {
	s := o.sequence.Add(1)
	if s >= o.maxSequence {
		o.sequence.Store(0)
		return 0
	}
	return s
}

func newOrder(maxSequence uint64) Generator {
	return &order{
		maxSequence: maxSequence,
		sequence:    atomic.Uint64{},
	}
}
