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
	for {
		s := o.sequence.Load()
		next := s + 1
		if next >= o.maxSequence {
			if o.sequence.CompareAndSwap(s, 0) {
				return 0
			}
			continue
		}
		if o.sequence.CompareAndSwap(s, next) {
			return next
		}
	}
}

func newOrder(maxSequence uint64) Generator {
	return &order{
		maxSequence: maxSequence,
		sequence:    atomic.Uint64{},
	}
}
