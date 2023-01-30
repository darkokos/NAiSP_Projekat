package bloomfilter

import "math"

// Funkcija racuna broj bitova koji je potrebno smestiti u bloom filter na
// osnovu ocekivanog broja elemenata i false-positive verovatanoce.
func CalculateM(expectedElements int, falsePositiveRate float64) uint {
	return uint(math.Ceil(float64(expectedElements) * math.Abs(math.Log(falsePositiveRate)) / math.Pow(math.Log(2), float64(2))))
}

// Funkcija racuna optimalan broj hes funkcija za bloom filter na osnovu
// ocekivanog broja elemenata i duzine niza bitova u bloom filteru.
func CalculateK(expectedElements int, m uint) uint {
	return uint(math.Ceil((float64(m) / float64(expectedElements)) * math.Log(2)))
}
