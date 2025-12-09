package sequence

import (
	"math/rand"
	"time"
)

var _ Generator = &zipf{}

type zipf struct {
	maxSequence uint64
	zipfRand    *rand.Zipf
}

func (z *zipf) Next() uint64 {
	return z.zipfRand.Uint64()
}

func newZipf(maxSequence uint64) Generator {
	source := rand.NewSource(time.Now().UnixNano())
	zipfRand := rand.NewZipf(rand.New(source), 1.1, 1, maxSequence-1)
	return &zipf{
		maxSequence: maxSequence,
		zipfRand:    zipfRand,
	}
}
