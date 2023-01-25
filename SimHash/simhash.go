package simhash

import (
	"crypto/md5"
	"fmt"
	"regexp"
	"strings"
)

func CalculateSimilarity(distance int) float64 {
	return 1 - (float64(distance) / 128)
}

func SimHashTest() {

	simhash1 := NewSimHash("Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book. It has survived not only five centuries, but also the leap into electronic typesetting, remaining essentially unchanged. It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum.")
	simhash2 := NewSimHash("Contrary to popular belief, Lorem Ipsum is not simply random text. It has roots in a piece of classical Latin literature from 45 BC, making it over 2000 years old. Richard McClintock, a Latin professor at Hampden-Sydney College in Virginia, looked up one of the more obscure Latin words, consectetur, from a Lorem Ipsum passage, and going through the cites of the word in classical literature, discovered the undoubtable source. Lorem Ipsum comes from sections 1.10.32 and 1.10.33 of 'de Finibus Bonorum et Malorum' (The Extremes of Good and Evil) by Cicero, written in 45 BC. This book is a treatise on the theory of ethics, very popular during the Renaissance. The first line of Lorem Ipsum, 'Lorem ipsum dolor sit amet..', comes from a line in section 1.10.32.")
	simhash1copy := NewSimHash("Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book. It has survived not only five centuries, but also the leap into electronic typesetting, remaining essentially unchanged. It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum.")
	simhash2similar := NewSimHash("Contrary to SLICNO belief, Lorem Ipsum is not simply SLICNO text. It has roots in a SLICNO of classical Latin literature from 45 BC, SLICNO it over 2000 years old. Richard McClintock, a Latin SLICNO at Hampden-Sydney College in Virginia, looked up one of the more obscure Latin words, consectetur, from a Lorem Ipsum SLICNO, and going through the cites of the word in classical literature, discovered the SLICNO source. Lorem Ipsum comes from sections 1.10.32 and 1.10.33 of 'de Finibus Bonorum et Malorum' (The Extremes of Good and Evil) by Cicero, written in 45 BC. This book is a treatise on the SLICNO of ethics, very popular during the Renaissance. The first SLICNO of Lorem Ipsum, 'Lorem ipsum dolor sit amet..', comes from a SLICNO in section 1.10.32.")

	fmt.Println("Simhash1 Fingerprint: ", simhash1.GetFingerprint())
	fmt.Println("Simhash2 Fingerprint: ", simhash2.GetFingerprint())
	fmt.Println("Simhash1copy Fingerprint: ", simhash1copy.GetFingerprint())
	fmt.Println("Simhash2similar Fingerprint: ", simhash2similar.GetFingerprint())

	distance := simhash1.CalculateDistance(simhash2)
	fmt.Println("Hamming distance (1, 2):", distance)

	similarity := CalculateSimilarity(distance)
	fmt.Println("Similarity (1, 2):", similarity)

	distance = simhash1.CalculateDistance(simhash1copy)
	fmt.Println("Hamming distance (1, 1copy):", distance)

	similarity = CalculateSimilarity(distance)
	fmt.Println("Similarity (1, 1copy):", similarity)

	distance = simhash2.CalculateDistance(simhash2similar)
	fmt.Println("Hamming distance (2, 2similar):", distance)

	similarity = CalculateSimilarity(distance)
	fmt.Println("Similarity (2, 2similar):", similarity)

}

func (s *SimHash) GetFingerprint() string {
	fp := ""
	for i := 0; i < 128; i++ {
		fp = fp + fmt.Sprint(s.Fingerprint[i])
	}
	return fp
}

type SimHash struct {
	Words       []string
	Hashes      [][128]byte
	Count       [128]int
	Fingerprint [128]byte
}

func NewSimHash(text string) *SimHash {
	sh := &SimHash{}
	sh.RemoveStopWords(text)
	sh.HashWords()
	sh.CountBits()
	sh.CalculateFingerprint()
	return sh
}

func (s *SimHash) RemoveStopWords(text string) {
	// split-ujemo tekst i uklanjamo specijalne karaktere
	for _, word := range strings.Fields(text) {
		word = regexp.MustCompile(`[^a-zA-Z0-9 ]+`).ReplaceAllString(word, "")
		s.Words = append(s.Words, word)
	}
}

func (s *SimHash) HashWords() {
	// hash-ujemo svaku rec sa MD5 hash algoritmom
	for _, word := range s.Words {
		hash := md5.Sum([]byte(word))
		// posto je MD5 hash 128 bitan ali vraca 16 bajtova, moramo da ga pretvorimo hash u 128 bita
		var bits [128]byte
		for i := 0; i < 128; i++ {
			// ekstraktujemo svaki bit iz bajta i dodajemo na odgovarajcu poziciju
			bits[i] = hash[i/8] >> uint(i%8) & 1
		}
		// dodajemo hash u niz hash-ova (svaki hash odgovara reci sa istim index-om)
		s.Hashes = append(s.Hashes, bits)
	}
}

func (s *SimHash) CountBits() {
	// racunamo zbir svih bitova (ako je 1 +1, ako je 0 -1) i smestano sumu na odgovarajuci index za koji ta suma odgovara
	for _, hash := range s.Hashes {
		for i := 0; i < 128; i++ {
			if hash[i] == 1 {
				s.Count[i]++
			} else {
				s.Count[i]--
			}
		}
	}
}

func (s *SimHash) CalculateFingerprint() {
	// racunamo fingerprint na osnovu sume vrednosti bitova ( > 0 stavljamo 1, ostalo stavljamo 0)
	for i := 0; i < 128; i++ {
		if s.Count[i] >= 0 {
			s.Fingerprint[i] = 1
		} else {
			s.Fingerprint[i] = 0
		}
	}
}

func (s *SimHash) CalculateDistance(simhash *SimHash) int {
	// racunamo hammingovu razdaljinu izmedju dva fingerprint-a
	var distance int
	for i := 0; i < 128; i++ {
		if s.Fingerprint[i] != simhash.Fingerprint[i] {
			distance++
		}
	}
	return distance
}
