package bloomfilter

// Implementacija bloom filtera
// Duzina seed-a jedne hes funkcije je 32 bajta
type BloomFilter struct {
	HashFunctions     []HashWithSeed
	BitField          []byte
	BitArrayLen       uint
	HashFunctionCount uint
}

// Maske za izolovanje pojedinacnih bitova
var masks [8]byte = [8]byte{0b00000001, 0b00000010, 0b00000100, 0b00001000, 0b00010000, 0b00100000, 0b01000000, 0b10000000}

// Funkcija pravi bloom filter sa poljem bitova duzine bitArrayLen i
// hashFunnctionCount hes funkcija.
func CreateBloomFilter(bitArrayLen uint, hashFunctionCount uint) BloomFilter {
	BitField := make([]byte, bitArrayLen/8+bitArrayLen%8) // Ako broj bitova nije deljiv sa 8, zaokruzujemo broj bajtova na gore da bi se alociralo dovoljno prostora.

	hashFunctions := CreateHashFunctions(hashFunctionCount)

	bloomFilter := BloomFilter{HashFunctions: hashFunctions, BitField: BitField, BitArrayLen: bitArrayLen, HashFunctionCount: hashFunctionCount}
	return bloomFilter
}

// Funkcija kreira bloom filter koji ima postavljene optimalne parametre za
// dati ocekivan broj elemenata i false-positive verovatnocu.
// Videti [CalculateM] i [CalculateK].
func CreateBloomFilterBasedOnParams(expectedElements int, falsePositiveRate float64) BloomFilter {

	bitArrayLen := CalculateM(expectedElements, falsePositiveRate)
	hashFnCount := CalculateK(expectedElements, bitArrayLen)

	return CreateBloomFilter(bitArrayLen, hashFnCount)
}

// Funkcija dekomponuje indeks bita u polju bitova na indeks bajta i
// indeks bita u tom bajtu.
func bitAndByteIndex(bitFieldIndex uint64, bitFieldLen uint64) (byteIndex uint64, bitIndex uint64) {

	bitIndex = bitFieldIndex % bitFieldLen

	byteIndex = bitIndex / 8
	bitIndex = bitIndex % 8

	return byteIndex, bitIndex
}

// Funkcija dodaje element u bloom filter.
func (bloomFilter BloomFilter) Add(key []byte) {

	//bitIndices := make(uint64[], bloomFilter.HashFunctionCount)
	for _, hashFn := range bloomFilter.HashFunctions {
		byteIndex, bitIndex := bitAndByteIndex(hashFn.Hash(key), uint64(bloomFilter.BitArrayLen))

		bloomFilter.BitField[byteIndex] |= masks[bitIndex]

	}
}

// Funkcija proverava da li je element mozda prisutan u bloom filteru.
// Vraca true ako element mozda jeste prisutan, a false ako sigurno nije.
func (bloomFilter BloomFilter) Find(key []byte) bool {

	for _, hashFn := range bloomFilter.HashFunctions {
		byteIndex, bitIndex := bitAndByteIndex(hashFn.Hash(key), uint64(bloomFilter.BitArrayLen))

		couldItExist := bloomFilter.BitField[byteIndex] & masks[bitIndex]

		if couldItExist == 0 {
			return false
		}
	}

	return true
}
