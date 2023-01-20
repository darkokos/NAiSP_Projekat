package bloomfilter

type BloomFilter struct {
	HashFunctions []HashWithSeed
	BitField []byte
	BitArrayLen uint
	HashFunctionCount uint
}

// Maske za izolovanje pojedinacnih bitova
var masks [8]byte = [8]byte{0b00000001, 0b00000010, 0b00000100, 0b00001000, 0b00010000, 0b00100000, 0b01000000, 0b10000000}

func CreateBloomFilter(bitArrayLen uint, hashFunctionCount uint) BloomFilter {
	BitField := make([]byte, bitArrayLen/8)
	hashFunctions := CreateHashFunctions(hashFunctionCount)

	bloomFilter := BloomFilter{HashFunctions: hashFunctions, BitField: BitField, BitArrayLen: bitArrayLen, HashFunctionCount: hashFunctionCount}
	return bloomFilter
}


func bitAndByteIndex(bitFieldIndex uint64, bitFieldLen uint64) (byteIndex uint64, bitIndex uint64) {
	bitIndex = bitFieldIndex % bitFieldLen
	//fmt.Println("Bit checking")
	//fmt.Println(bitIndex)

	byteIndex = bitIndex / 8
	bitIndex = bitIndex % 8 - 1

	//fmt.Println(bitIndex)
	//fmt.Println(byteIndex)
	//fmt.Println("------")

	return byteIndex, bitIndex
}

func (bloomFilter BloomFilter) add(key []byte) {
	//bitIndices := make(uint64[], bloomFilter.HashFunctionCount)
	for _, hashFn := range bloomFilter.HashFunctions {
		byteIndex, bitIndex := bitAndByteIndex(hashFn.Hash(key), uint64(bloomFilter.BitArrayLen))

		bloomFilter.BitField[byteIndex] |= masks[bitIndex]  

	}
}

func (bloomFilter BloomFilter) find(key []byte) bool{
	for _, hashFn := range bloomFilter.HashFunctions {
		byteIndex, bitIndex := bitAndByteIndex(hashFn.Hash(key), uint64(bloomFilter.BitArrayLen))

		couldItExist := bloomFilter.BitField[byteIndex] & masks[bitIndex]  
		
		if (couldItExist == 0) {
			return false
		}
	}

	return true
}