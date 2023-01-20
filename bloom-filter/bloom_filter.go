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
	/*
		Funkcija pravi bloom filter sa poljem bitova duzine bitArrayLen i
		hashFunnctionCount hes funkcija.
	*/
	BitField := make([]byte, bitArrayLen/8)
	hashFunctions := CreateHashFunctions(hashFunctionCount)

	bloomFilter := BloomFilter{HashFunctions: hashFunctions, BitField: BitField, BitArrayLen: bitArrayLen, HashFunctionCount: hashFunctionCount}
	return bloomFilter
}

//TODO: Kreiranje bloom filtera na osnovu broja elemenata i false-positive rate-a

func bitAndByteIndex(bitFieldIndex uint64, bitFieldLen uint64) (byteIndex uint64, bitIndex uint64) {
	/*
		Funkcija dekomponuje indeks bita u polju bitova na indeks bajta i 
		indeks bita u tom bajtu.
	*/

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
	/*
		Funkcija dodaje element u bloom filter.
	*/

	//bitIndices := make(uint64[], bloomFilter.HashFunctionCount)
	for _, hashFn := range bloomFilter.HashFunctions {
		byteIndex, bitIndex := bitAndByteIndex(hashFn.Hash(key), uint64(bloomFilter.BitArrayLen))

		bloomFilter.BitField[byteIndex] |= masks[bitIndex]  

	}
}

func (bloomFilter BloomFilter) find(key []byte) bool {
	/*
		Funkcija proverava da li je element mozda prisutan u bloom filteru.
		Vraca true ako element mozda jeste prisutan, a false ako sigurno nije.
	*/

	for _, hashFn := range bloomFilter.HashFunctions {
		byteIndex, bitIndex := bitAndByteIndex(hashFn.Hash(key), uint64(bloomFilter.BitArrayLen))

		couldItExist := bloomFilter.BitField[byteIndex] & masks[bitIndex]  
		
		if (couldItExist == 0) {
			return false
		}
	}

	return true
}