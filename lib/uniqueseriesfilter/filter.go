package uniqueseriesfilter

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
	"github.com/axiomhq/hyperloglog"
	"github.com/cespare/xxhash/v2"
)

const hashesCount = 4
const bitsPerItem = 16
const defaultSize = 1_000_000

type Filter struct {
	maxSize uint64
	tooMany atomic.Bool

	bits atomic.Pointer[[]uint64]

	skMu   sync.Mutex
	prevSk *hyperloglog.Sketch
	currSk *hyperloglog.Sketch

	stopCh chan struct{}
}

func New(maxSize int) *Filter {
	if maxSize == 0 {
		maxSize = 40_000_000
	}

	f := &Filter{
		maxSize: uint64(maxSize),
		currSk:  newSketch(),

		stopCh: make(chan struct{}),
	}

	bits := newBits(defaultSize)
	f.bits.Store(&bits)

	go f.doRefresh()

	return f
}

func (f *Filter) Filter(srcTss, resTss []prompb.TimeSeries) []prompb.TimeSeries {
	if f.tooMany.Load() {
		for i := range srcTss {
			ts := srcTss[i]

			h := getLabelsHash(ts.Labels)

			f.skMu.Lock()
			f.currSk.InsertHash(h)
			f.skMu.Unlock()
		}

		return srcTss
	}

	for i := range srcTss {
		ts := srcTss[i]

		h := getLabelsHash(ts.Labels)

		f.skMu.Lock()
		f.currSk.InsertHash(h)
		f.skMu.Unlock()

		if !f.add(h) {
			continue
		}

		resTss = append(resTss, ts)
	}

	return resTss
}

func (f *Filter) Stop() {
	close(f.stopCh)
}

func (f *Filter) doRefresh() {
	t := time.NewTicker(time.Minute / 10)
	defer t.Stop()

	iter := 1
	for {
		select {
		case <-t.C:
			// partially reset some bits
			bits := *f.bits.Load()
			chunk := iter % bitsPerItem
			for i := chunk; i < len(bits); i += 10 {
				atomic.StoreUint64(&bits[i], 0)
			}

			// rotate sketches; maybe grow bits size
			if iter%bitsPerItem == 0 {
				f.skMu.Lock()
				prevSk := f.prevSk
				if prevSk == nil {
					prevSk = newSketch()
				}
				prevSk.Reset()
				f.prevSk = f.currSk
				f.currSk = prevSk

				sk := newSketch()
				_ = sk.Merge(f.currSk)
				_ = sk.Merge(f.prevSk)

				f.skMu.Unlock()

				bits := *f.bits.Load()
				estimatedSize := sk.Estimate()
				currSize := bitsSize(bits)
				if estimatedSize > f.maxSize {
					if !f.tooMany.Load() {
						logger.Infof("cardinality bigger than %d, pass through everything", f.maxSize)
						f.tooMany.Store(true)
						bits := newBits(defaultSize)
						f.bits.Store(&bits)
					}
				} else if estimatedSize > currSize {
					f.tooMany.Store(false)
					newSize := 2 * estimatedSize
					logger.Infof("resize cardinality filter from %d to %d", currSize, newSize)
					bits := newBits(newSize)
					f.bits.Store(&bits)
				} else if estimatedSize < currSize/4 {
					f.tooMany.Store(false)
					newSize := currSize/2
					logger.Infof("resize cardinality filter from %d to %d", currSize, newSize)
					bits := newBits(newSize)
					f.bits.Store(&bits)
				}
			}

			iter++
		case <-f.stopCh:
			return
		}
	}
}

func (f *Filter) add(h uint64) bool {
	bits := *f.bits.Load()
	maxBits := uint64(len(bits)) * 64
	bp := (*[8]byte)(unsafe.Pointer(&h))
	b := bp[:]
	isNew := false
	for range hashesCount {
		hi := xxhash.Sum64(b)
		h++
		idx := hi % maxBits
		i := idx / 64
		j := idx % 64
		mask := uint64(1) << j
		w := atomic.LoadUint64(&bits[i])
		for (w & mask) == 0 {
			wNew := w | mask
			// The wNew != w most of the time, so there is no need in using atomic.LoadUint64
			// in front of atomic.CompareAndSwapUint64 in order to try avoiding slow inter-CPU synchronization.
			if atomic.CompareAndSwapUint64(&bits[i], w, wNew) {
				isNew = true
				break
			}
			w = atomic.LoadUint64(&bits[i])
		}
	}
	return isNew
}

var labelsHashBufPool bytesutil.ByteBufferPool

func getLabelsHash(labels []prompb.Label) uint64 {
	bb := labelsHashBufPool.Get()
	b := bb.B[:0]
	for _, label := range labels {
		b = append(b, label.Name...)
		b = append(b, label.Value...)
	}
	h := xxhash.Sum64(b)
	bb.B = b
	labelsHashBufPool.Put(bb)
	return h
}

func newBits(maxItems uint64) []uint64 {
	bitsCount := maxItems * bitsPerItem
	return make([]uint64, (bitsCount+63)/64)
}

func bitsSize(bits []uint64) uint64 {
	return uint64((len(bits) - 63) * 64 / bitsPerItem)
}

func newSketch() *hyperloglog.Sketch {
	sk, err := hyperloglog.NewSketch(14, true)
	if err != nil {
		panic(fmt.Sprintf("BUG: hyperloglog: new sketch: %s", err))
	}
	return sk
}
