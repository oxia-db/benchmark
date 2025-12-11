package sequence

import (
	"math/rand"
	"sync"
	"time"
)

var _ Generator = &zipf{}

type zipf struct {
	maxSequence uint64
	zipfRand    *rand.Zipf
	mu          sync.Mutex
}

func (z *zipf) Next() uint64 {
	z.mu.Lock()
	value := z.zipfRand.Uint64()
	z.mu.Unlock()
	return value
}

func newZipf(maxSequence uint64) Generator {
	source := rand.NewSource(time.Now().UnixNano())
	zipfRand := rand.NewZipf(rand.New(source), 1.1, 1, maxSequence-1)
	return &zipf{
		maxSequence: maxSequence,
		zipfRand:    zipfRand,
		mu:          sync.Mutex{},
	}
}
