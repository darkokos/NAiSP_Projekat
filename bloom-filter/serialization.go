package bloomfilter

import (
	"encoding/binary"
)

// Funkcija serijalizuje bloom filter u niz bajtova.
//
// Format niza bajtova:
//
// Duzina niza bitova m (4B) | Broj hes funkcija k (4B) | Seed-ovi hes funkcija (k * 4B) | Polje bitova (m bitova, ali u bajtovima, zaokruzeno na gore)
func (bloomFilter *BloomFilter) Serialize() []byte {

	output := make([]byte, 4+4+bloomFilter.HashFunctionCount*4+bloomFilter.BitArrayLen/8+bloomFilter.BitArrayLen%8)

	binary.BigEndian.PutUint32(output[0:4], uint32(bloomFilter.BitArrayLen))
	binary.BigEndian.PutUint32(output[4:8], uint32(bloomFilter.HashFunctionCount))

	for idx, hashFunction := range bloomFilter.HashFunctions {
		copy(output[8+(idx*4):8+(idx*4)+4], hashFunction.Seed)
	}

	copy(output[8+bloomFilter.HashFunctionCount*4:], bloomFilter.BitField)

	return output
}
