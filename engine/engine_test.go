package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestEngineSimple(t *testing.T) {
	// Brisanje fajlova od proslih testova
	// Brisemo sve fajlove sa imenima oblika *.db
	files, err := filepath.Glob("*.db")
	if err != nil {
		panic(err)
	}
	for _, f := range files {
		if err := os.Remove(f); err != nil {
			panic(err)
		}
	}

	files, err = filepath.Glob("*.txt")
	if err != nil {
		panic(err)
	}
	for _, f := range files {
		if err := os.Remove(f); err != nil {
			panic(err)
		}
	}

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

func TestEngineWithDeletions(t *testing.T) {
	// Brisanje fajlova od proslih testova
	// Brisemo sve fajlove sa imenima oblika *.db
	files, err := filepath.Glob("*.db")
	if err != nil {
		panic(err)
	}
	for _, f := range files {
		if err := os.Remove(f); err != nil {
			panic(err)
		}
	}

	files, err = filepath.Glob("*.txt")
	if err != nil {
		panic(err)
	}
	for _, f := range files {
		if err := os.Remove(f); err != nil {
			panic(err)
		}
	}

	db := GetNewDB()
	db.Put("102", []byte{102})
	if db.Delete("102") == false {
		t.Fatalf("Trebalo da je obrise")
	}

	if db.Get("102") != nil {
		fmt.Println(db.Get("101"))
		t.Fatalf("Nije trebalo da nadje ovo")
	}

	db.Put("101", []byte{101})
	db.Put("103", []byte{103})
	db.Put("104", []byte{104})

	if db.Get("102") != nil {
		fmt.Println(db.Get("102"))
		t.Fatalf("Nije trebalo da nadje ovo")
	}

	if db.Get("101") == nil {
		t.Fatalf("Trebalo je da nadje ovo")
	}

}
