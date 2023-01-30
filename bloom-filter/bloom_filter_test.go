package bloomfilter

import (
	"testing"
)

func TestWords(t *testing.T) {
	// Test primer preuzet sa https://www.geeksforgeeks.org/bloom-filters-introduction-and-python-implementation/

	// Reci koje se dodaju
	word_present := []string{"abound", "abounds", "abundance", "abundant", "accessible",
		"bloom", "blossom", "bolster", "bonny", "bonus", "bonuses",
		"coherent", "cohesive", "colorful", "comely", "comfort",
		"gems", "generosity", "generous", "generously", "genial"}

	// Reci koje se ne dodaju
	word_absent := []string{"bluff", "cheater", "hate", "war", "humanity",
		"racism", "hurt", "nuke", "gloomy", "facebook",
		"geeksforgeeks", "twitter"}

	falsePositiveRate := 0.05

	bloomFilter := CreateBloomFilterBasedOnParams(len(word_present), falsePositiveRate)
	t.Log("Duzina niza bitova: ", bloomFilter.BitArrayLen)
	t.Log("Broj hes funkcija: ", bloomFilter.HashFunctionCount)
	t.Log("False-positive rate: ", falsePositiveRate)

	for _, word := range word_present {
		bloomFilter.add([]byte(word))
	}

	for _, word := range word_present {
		found := bloomFilter.find([]byte(word))
		if !found {
			t.Fatalf("%s je trebao da bude nadjen, a nije", word)
		} else {
			t.Log(word, " nadjen")
		}
	}

	for _, word := range word_absent {
		found := bloomFilter.find([]byte(word))
		if !found {
			t.Log(word, " nije nadjen")
		} else {
			t.Log(word, " je false-positive")
		}
	}

}

func TestSerializationLength(t *testing.T) {
	expectedElements := 99
	falsePositiveRate := 0.01

	bloomFilter := CreateBloomFilterBasedOnParams(expectedElements, falsePositiveRate)

	expectedLength := bloomFilter.BitArrayLen/8 + bloomFilter.BitArrayLen%8 + bloomFilter.HashFunctionCount*32 + 4 + 4
	actualLength := len(bloomFilter.Serialize())

	if len(bloomFilter.Serialize()) != int(expectedLength) {
		t.Fatalf("Duzina serijalizovanog bloom filtera nije ispravna. Ocekivano: %d Dobijena duzina: %d", expectedLength, actualLength)
	}
}

func TestSerializedStructEquality(t *testing.T) {
	// Reci koje se dodaju
	word_present := []string{"abound", "abounds", "abundance", "abundant", "accessible",
		"bloom", "blossom", "bolster", "bonny", "bonus", "bonuses",
		"coherent", "cohesive", "colorful", "comely", "comfort",
		"gems", "generosity", "generous", "generously", "genial"}

	falsePositiveRate := 0.05

	bloomFilter := CreateBloomFilterBasedOnParams(len(word_present), falsePositiveRate)
	t.Log("Duzina niza bitova: ", bloomFilter.BitArrayLen)
	t.Log("Broj hes funkcija: ", bloomFilter.HashFunctionCount)
	t.Log("False-positive rate: ", falsePositiveRate)

	for _, word := range word_present {
		bloomFilter.add([]byte(word))
	}

	deserializedBloomFilter := Deserialize(bloomFilter.Serialize())

	if deserializedBloomFilter.BitArrayLen != bloomFilter.BitArrayLen {
		t.Fatalf("Brojevi koji treba da su duizne bloom filtera se ne poklapaju %d != %d", bloomFilter.BitArrayLen, deserializedBloomFilter.BitArrayLen)
	}

	if len(deserializedBloomFilter.BitField) != len(bloomFilter.BitField) {
		t.Fatalf("Duzine polja bitova bloom filtera i serijalizovanog bloom filtera se ne poklapaju %d != %d", len(bloomFilter.BitField), len(deserializedBloomFilter.BitField))
	}

	if deserializedBloomFilter.HashFunctionCount != bloomFilter.HashFunctionCount {
		t.Fatalf("Brojevi hes funkcija se ne poklapaju. Ocekivano: %d Deserijalizovano: %d", bloomFilter.HashFunctionCount, deserializedBloomFilter.HashFunctionCount)
	}

	for i := 0; i < len(deserializedBloomFilter.HashFunctions); i++ {
		originalSeed := bloomFilter.HashFunctions[i].Seed
		serializedSeed := deserializedBloomFilter.HashFunctions[i].Seed

		if len(originalSeed) != len(serializedSeed) {
			t.Fatalf("%d-ti seed-ovi se ne poklapaju %d != %d", i+1, len(originalSeed), len(serializedSeed))
		}

		for j := 0; j < len(originalSeed); j++ {
			if originalSeed[j] != serializedSeed[j] {
				t.Fatalf("%d-ti seed-ovi se ne poklapaju %d != %d", i+1, originalSeed, serializedSeed)
			}
		}
	}

	for i := 0; i < len(deserializedBloomFilter.BitField); i++ {
		if deserializedBloomFilter.BitField[i] != bloomFilter.BitField[i] {
			t.Fatalf("%d-ti bajt bloom filtera i serijalizovanog bloom filtera se ne poklapju", i)
		}
	}
}

func TestSerializeDeserializeWithOperations(t *testing.T) {
	// Reci koje se dodaju
	word_present := []string{"abound", "abounds", "abundance", "abundant", "accessible",
		"bloom", "blossom", "bolster", "bonny", "bonus", "bonuses",
		"coherent", "cohesive", "colorful", "comely", "comfort",
		"gems", "generosity", "generous", "generously", "genial"}

	// Reci koje se ne dodaju
	word_absent := []string{"bluff", "cheater", "hate", "war", "humanity",
		"racism", "hurt", "nuke", "gloomy", "facebook",
		"geeksforgeeks", "twitter"}

	falsePositiveRate := 0.05

	bloomFilter := CreateBloomFilterBasedOnParams(len(word_present), falsePositiveRate)
	t.Log("Duzina niza bitova: ", bloomFilter.BitArrayLen)
	t.Log("Broj hes funkcija: ", bloomFilter.HashFunctionCount)
	t.Log("False-positive rate: ", falsePositiveRate)

	for _, word := range word_present {
		bloomFilter.add([]byte(word))
	}

	bloomFilter = Deserialize(bloomFilter.Serialize())

	for _, word := range word_present {
		found := bloomFilter.find([]byte(word))
		if !found {
			t.Fatalf("%s je trebao da bude nadjen, a nije", word)
		} else {
			t.Log(word, " nadjen")
		}
	}

	for _, word := range word_absent {
		found := bloomFilter.find([]byte(word))
		if !found {
			t.Log(word, " nije nadjen")
		} else {
			t.Log(word, " je false-positive")
		}
	}
}
