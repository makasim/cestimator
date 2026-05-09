package cardinalityfilter

import (
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
	"github.com/cespare/xxhash/v2"
)

const hashesCount = 4
const bitsPerItem = 16
const resetParts = 10

type Filter struct {
	rt    *time.Ticker
	chunk int

	maxItems int
	bits     []uint64
}

func NewFilter(maxItems int, refreshDur time.Duration) *Filter {
	bitsCount := maxItems * bitsPerItem
	bits := make([]uint64, (bitsCount+63)/64)
	return &Filter{
		maxItems: maxItems,
		bits:     bits,
		rt:       time.NewTicker(refreshDur / resetParts),
	}
}

func (f *Filter) filter(srcTss, resTss []prompb.TimeSeries) []prompb.TimeSeries {
	select {
	case <-f.rt.C:
		f.partialReset()
	default:
	}

	for i := range srcTss {
		ts := srcTss[i]

		h := getLabelsHash(ts.Labels)
		if !f.add(h) {
			continue
		}

		resTss = append(resTss, ts)
	}

	return resTss
}

// partialReset clears 1/resetParts of the bits on each call.
// After resetParts consecutive calls with no concurrent writes, all bits are zero.
func (f *Filter) partialReset() {
	bits := f.bits
	for i := f.chunk; i < len(bits); i += resetParts {
		atomic.StoreUint64(&bits[i], 0)
	}
	f.chunk = (f.chunk + 1) % resetParts
}

func (f *Filter) add(h uint64) bool {
	bits := f.bits
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
