package hyperloglog

import (
	"fmt"
	"sync"
)

type SketchPool struct {
	p uint8
	s bool

	mu sync.Mutex
	sp *sync.Pool
}

func NewSketchPool(precision uint8, sparse bool) *SketchPool {
	return &SketchPool{
		p: precision,
		s: sparse,

		sp: &sync.Pool{},
	}
}

func (skp *SketchPool) Get() (*Sketch, error) {
	skp.mu.Lock()
	defer skp.mu.Unlock()

	sk := skp.sp.Get()
	if sk == nil {
		return NewSketch(skp.p, skp.s)
	}

	return sk.(*Sketch), nil
}

func (skp *SketchPool) MustGet() *Sketch {
	sk, err := skp.Get()
	if err != nil {
		panic(err)
	}

	return sk
}

func (skp *SketchPool) Put(sk *Sketch) error {
	if sk == nil {
		return nil
	}
	if sk.p != skp.p {
		return fmt.Errorf("sketch with precision %d could not be placed into pool with precision %d", sk.p, skp.p)
	}

	sk.Reset()

	skp.mu.Lock()
	skp.sp.Put(sk)
	skp.mu.Unlock()
	return nil
}

func (skp *SketchPool) MustPut(sk *Sketch) {
	if err := skp.Put(sk); err != nil {
		panic(err)
	}
}

type SketchPoolPool struct {
	sparse [15]*SketchPool
	normal [15]*SketchPool
}

func NewSketchPoolPool() *SketchPoolPool {
	skpp := &SketchPoolPool{}
	for i := 0; i < len(skpp.sparse); i++ {
		skpp.sparse[i] = NewSketchPool(uint8(i+4), true)
		skpp.normal[i] = NewSketchPool(uint8(i+4), false)
	}

	return skpp
}

func (skpp *SketchPoolPool) Get(precision uint8, sparse bool) (*Sketch, error) {
	if sparse {
		return skpp.sparse[precision-4].Get()
	}
	return skpp.normal[precision-4].Get()
}

func (skpp *SketchPoolPool) MustGet(precision uint8, sparse bool) *Sketch {
	if sparse {
		return skpp.sparse[precision-4].MustGet()
	}
	return skpp.normal[precision-4].MustGet()
}

func (skpp *SketchPoolPool) Put(sk *Sketch) error {
	if sk == nil {
		return nil
	}

	if sk.s {
		return skpp.sparse[sk.p-4].Put(sk)
	}
	return skpp.normal[sk.p-4].Put(sk)
}

func (skpp *SketchPoolPool) MustPut(sk *Sketch) {
	if sk == nil {
		return
	}

	if sk.s {
		skpp.sparse[sk.p-4].MustPut(sk)
		return
	}
	skpp.normal[sk.p-4].MustPut(sk)
}
