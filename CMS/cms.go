package CMS

import (
	"encoding/binary"
	"math"
)

type CMS struct {
	table     [][]uint
	k         uint
	m         uint
	precision float64
	certainty float64
	hashes    []HashWithSeed
}

func (cms *CMS) Init(precision float64, certainty float64) {
	(*cms).precision = precision
	(*cms).certainty = certainty
	(*cms).k = CalculateK(certainty)
	(*cms).m = CalculateM(precision)
	(*cms).table = make([][]uint, (*cms).k)
	for i := range (*cms).table {
		(*cms).table[i] = make([]uint, (*cms).m)
	}
	(*cms).hashes = CreateHashFunctions((*cms).k)
}
func (cms *CMS) Add(key []byte) {
	var j uint64
	for i := range (*cms).hashes {
		j = (*cms).hashes[i].Hash(key)
		(*cms).table[i][j] += 1
	}
}
func (cms *CMS) Read(key []byte) uint {
	min := uint(0)
	var j uint64
	for i := range (*cms).hashes {
		j = (*cms).hashes[i].Hash(key)
		if (*cms).table[i][j] < min {
			min = (*cms).table[i][j]
		}
	}
	return min
}
func (cms *CMS) Serialize() []byte {
	var ret []byte

	ret = append(ret, serializeFloat((*cms).precision)...)
	ret = append(ret, serializeFloat((*cms).certainty)...)

	for i := range (*cms).table {
		for j := range (*cms).table[i] {
			ret = append(ret, serializeUint((uint32)((*cms).table[i][j]))...)
		}
	}

	return ret
}
func serializeUint(x uint32) []byte {
	a := make([]byte, 4)
	binary.BigEndian.PutUint32(a, x)
	return a
}
func serializeFloat(f float64) []byte {
	var buf [8]byte
	kn := math.Float64bits(f)

	for i := 0; i < 8; i++ {
		buf[i] = byte(kn >> (7 - i) * 8)
	}

	return buf[:]
}
