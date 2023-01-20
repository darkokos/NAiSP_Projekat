package bloomfilter

import (
	"encoding/binary"
)

// Funkcija serijalizuje bloom filter u niz bajtova.
//
// Format niza bajtova:
//
// Duzina niza bitova m (4B) | Broj hes funkcija k (4B) | Seed-ovi hes funkcija (k * 32B) | Polje bitova (m bitova, ali u bajtovima, zaokruzeno na gore)
func (bloomFilter *BloomFilter) Serialize() []byte {

	output := make([]byte, 4+4+bloomFilter.HashFunctionCount*32+bloomFilter.BitArrayLen/8+bloomFilter.BitArrayLen%8)

	binary.BigEndian.PutUint32(output[0:4], uint32(bloomFilter.BitArrayLen))
	binary.BigEndian.PutUint32(output[4:8], uint32(bloomFilter.HashFunctionCount))

	for idx, hashFunction := range bloomFilter.HashFunctions {
		copy(output[8+(idx*32):8+(idx*32)+32], hashFunction.Seed)
	}

	copy(output[8+bloomFilter.HashFunctionCount*32:], bloomFilter.BitField)

	return output
}

// Funkcija deserijalizuje bloom filter iz prosledjenog niza bajtova.
func Deserialize(bytes []byte) BloomFilter {
	bitArrayLen := binary.BigEndian.Uint32(bytes[0:4])
	hashFunctionCount := binary.BigEndian.Uint32(bytes[4:8])

	hashFunctions := make([]HashWithSeed, hashFunctionCount)

	for i := uint32(0); i < hashFunctionCount; i++ {
		hashFunctions[i] = HashWithSeed{Seed: bytes[8+(i*32) : 8+(i*32)+32]}
	}

	return BloomFilter{BitArrayLen: uint(bitArrayLen), HashFunctionCount: uint(hashFunctionCount), HashFunctions: hashFunctions, BitField: bytes[8+hashFunctionCount*32:]}
}
