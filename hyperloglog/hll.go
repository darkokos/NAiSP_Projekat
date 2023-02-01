package hyperloglog

import (
	"math"
	"math/bits"
)

const (
	HLL_MIN_PRECISION = 4
	HLL_MAX_PRECISION = 16
)

type HLL struct {
	m            uint64
	p            uint8
	reg          []uint8
	hashFunction HashWithSeed
}

func (hll *HLL) Estimate() float64 {
	sum := 0.0
	for _, val := range hll.reg {
		sum += math.Pow(math.Pow(2.0, float64(val)), -1)
	}

	alpha := 0.7213 / (1.0 + 1.079/float64(hll.m))
	estimation := alpha * math.Pow(float64(hll.m), 2.0) / sum
	emptyRegs := hll.emptyCount()
	if estimation <= 2.5*float64(hll.m) { // do small range correction
		if emptyRegs > 0 {
			estimation = float64(hll.m) * math.Log(float64(hll.m)/float64(emptyRegs))
		}
	} else if estimation > 1/30.0*math.Pow(2.0, 32.0) { // do large range correction
		estimation = -math.Pow(2.0, 32.0) * math.Log(1.0-estimation/math.Pow(2.0, 32.0))
	}
	return estimation
}

func (hll *HLL) emptyCount() int {
	sum := 0
	for _, val := range hll.reg {
		if val == 0 {
			sum++
		}
	}
	return sum
}

func CreateHLL(p uint8) HLL {
	m := uint64(1)
	for i := uint8(0); i < p; i++ {
		m *= 2
	}

	return HLL{m: m, p: p, reg: make([]uint8, m), hashFunction: CreateHashFunctions(1)[0]}
}

func (hll *HLL) hash(data []byte) uint64 {
	return hll.hashFunction.Hash(data)
}

func (hll *HLL) Add(key []byte) {
	hash := hll.hash(key)
	first_bit_mask := uint64(0x8000000000000000)
	mask := first_bit_mask

	for i := uint8(0); i < hll.p-1; i++ {
		mask >>= 1
		mask |= first_bit_mask
	}

	//fmt.Println(mask)

	bucket_index := (hash & mask) >> (64 - hll.p)
	new_reg_value := uint8(bits.TrailingZeros64(hash)) + 1
	//fmt.Println(hash)
	//fmt.Println(new_reg_value)
	if hll.reg[bucket_index] < new_reg_value {
		hll.reg[bucket_index] = new_reg_value
		//fmt.Println("filled")
	}

}

func (hll *HLL) add_simulation(hash uint64) {
	first_bit_mask := uint64(0x8000000000000000)
	mask := first_bit_mask

	for i := uint8(0); i < hll.p-1; i++ {
		mask >>= 1
		mask |= first_bit_mask
	}

	//fmt.Println(mask)

	bucket_index := (hash & mask) >> (64 - hll.p)
	new_reg_value := uint8(bits.TrailingZeros64(hash)) + 1
	//fmt.Println(hash)
	//fmt.Println(new_reg_value)
	if hll.reg[bucket_index] < new_reg_value {
		hll.reg[bucket_index] = new_reg_value
		//fmt.Println("filled")
	}

}
