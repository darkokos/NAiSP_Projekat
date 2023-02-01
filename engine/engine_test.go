package engine

import (
	"fmt"
	"testing"
)

func TestEngineSimple(t *testing.T) {
	db := GetNewDB()

	for i := 0; i < 100; i++ {
		ok := db.Put(fmt.Sprintf("%03d", i), []byte{uint8(i)})
		if !ok {
			t.Fatalf("Nesto je poslo po zlu")
		}
	}

	db.Put("101", []byte{101})
	if db.Get("101") == nil {
		t.Fatalf("Trebalo je da nadje ovo")
	}
	db.Put("101", []byte{250})
	db.Put("102", []byte{102})
	db.Put("103", []byte{103})
	db.Put("104", []byte{104}) // Ovde ce biti flush

	if db.Get("101")[0] != 250 {
		t.Fatalf("Pogresno je procitana vrednost")
	}

	for i := 0; i < 100; i++ {
		val := db.Get(fmt.Sprintf("%03d", i))
		if val == nil {
			t.Fatalf("Trebalo je da nadje ovo")
		}
	}

}
