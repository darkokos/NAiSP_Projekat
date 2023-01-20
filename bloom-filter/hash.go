package bloomfilter

import (
	"crypto/md5"
	"encoding/binary"
	"time"
)

// HashWithSeed predstavlja jednu hes funkciju
type HashWithSeed struct {
	Seed []byte
}

// Funkcija hesira podatke
func (h HashWithSeed) Hash(data []byte) uint64 {
	fn := md5.New()
	fn.Write(append(data, h.Seed...))
	return binary.BigEndian.Uint64(fn.Sum(nil))
}

// Funkcija kreira k razlicitih hes funkcija sa razlicitim seed-ovima
func CreateHashFunctions(k uint) []HashWithSeed {
	h := make([]HashWithSeed, k)
	ts := uint(time.Now().Unix())
	for i := uint(0); i < k; i++ {
		seed := make([]byte, 32)
		binary.BigEndian.PutUint32(seed, uint32(ts+i))
		hfn := HashWithSeed{Seed: seed}
		h[i] = hfn
	}
	return h
}
