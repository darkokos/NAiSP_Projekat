package memtable

import (
	"fmt"
	"testing"
)

func TestMemtableBTree(t *testing.T) {
	memTable := MakeBTreeMemTable(20)

	//Dodajemo 19 zapisa
	memTable.Update("2", []byte{0, 0, 0, 2})
	memTable.Update("3", []byte{0, 0, 0, 3})
	memTable.Update("1", []byte{0, 0, 0, 1})
	memTable.Update("4", []byte{0, 0, 0, 4})

	fmt.Println("Dodao 4")

	v, ok := memTable.Get("1")
	if !ok {
		t.Fatalf("Kljuc 1 bi trebalo da postoji")
	} else if v[3] != 1 {
		fmt.Println(v)
		t.Fatalf("Kljuc 1 je nadjen ali vrednost nije dobro iscitana %d", v[3])
	}

	memTable.Update("1", []byte{0, 0, 0, 10}) // Menjamo 1
	memTable.Update("22", []byte{0, 0, 0, 2})

	v, ok = memTable.Get("1")
	if !ok {
		t.Fatalf("Kljuc 1 bi trebalo da postoji")
	} else if v[3] != 10 {
		t.Fatalf("Kljuc 1 je nadjen ali vrednost nije dobro iscitana %d", v[3])
	}

	memTable.Update("33", []byte{0, 0, 0, 3})
	memTable.Update("11", []byte{0, 0, 0, 1})
	memTable.Update("44", []byte{0, 0, 0, 4})

	memTable.Update("111", []byte{0, 0, 0, 10})
	memTable.Update("222", []byte{0, 0, 0, 2})
	memTable.Update("333", []byte{0, 0, 0, 3})
	memTable.Update("444", []byte{0, 0, 0, 1})
	memTable.Update("1111", []byte{0, 0, 0, 4})
	memTable.Update("2222", []byte{0, 0, 0, 10})
	memTable.Update("3333", []byte{0, 0, 0, 2})
	memTable.Update("4444", []byte{0, 0, 0, 3})
	memTable.Update("11111", []byte{0, 0, 0, 1})
	memTable.Update("22222", []byte{0, 0, 0, 4})
	memTable.Update("33333", []byte{0, 0, 0, 10})

	v, ok = memTable.Get("2")
	fmt.Println(v)

	v, ok = memTable.Get("1")
	if !ok {
		t.Fatalf("Kljuc 1 bi trebalo da postoji")
	} else if v[3] != 10 {
		t.Fatalf("Kljuc 1 je nadjen ali vrednost nije dobro iscitana %d", v[3])
	}

	if memTable.IsDeleted("1") {
		t.Fatalf("Kljuc 1 ne bi trebalo da je obrisan")
	}

	_, ok = memTable.Get("5")
	if ok {
		t.Fatalf("Kljuc 5 nadjen pre nego sto je dodat")
	}

	memTable.Update("5", []byte{0, 0, 0, 5}) // Ovo ce izazvati flush

	_, ok = memTable.Get("2")
	if ok {
		t.Fatalf("Memtable bi trebalo da je bio flush-ovan")
	}

	fmt.Println("Drugi memtable")
	memTable.Update("5", []byte{0, 0, 0, 2})
	memTable.Update("36", []byte{0, 0, 0, 3})
	memTable.Update("231", []byte{0, 0, 0, 1})
	memTable.Update("33", []byte{0, 0, 0, 4})

	ok = memTable.Delete("37")
	if ok {
		t.Fatalf("Brisanje nepostojeceg kljuca ne bi trebalo da bude uspesno")
	}

	if memTable.IsDeleted("37") {
		t.Fatalf("Nepostojeci kljuc ne bi trebalo da je obrisan")
	}

	ok = memTable.Delete("33")
	if !ok {
		t.Fatalf("Brisanje postojeceg kljuca bi trebalo da bude uspesno")
	}

	_, ok = memTable.Get("33")
	if !ok {
		t.Fatalf("Kljuc 33 bi trebalo da je i dalje tu")
	} else if !memTable.IsDeleted("33") {
		t.Fatalf("Kljuc 33 bi trebalo da ima postavljen tombstone")
	}

	memTable.Update("11", []byte{0, 0, 0, 10})
	//memTable.Update("5", []byte{0,0,0,5})

	fmt.Println("Velicina memtable: ", memTable.data.Size())

	/*
		_, ok = memTable.Get("5")
		if ok {
			t.Fatalf("Memtable bi trebalo da je bio flush-ovan")
		}
	*/
}

func TestMemtableBTreeSmaller(t *testing.T) {
	memTable := MakeBTreeMemTable(5)

	memTable.Update("2", []byte{0, 0, 0, 2})
	memTable.Update("3", []byte{0, 0, 0, 3})
	memTable.Update("1", []byte{0, 0, 0, 1})

	memTable.Update("4", []byte{0, 0, 0, 4})

	memTable.Update("5", []byte{0, 0, 0, 5})

}

func TestMemtableBTreeMedium(t *testing.T) {
	memTable := MakeBTreeMemTable(30)

	memTable.Update("2", []byte{0, 0, 0, 2})
	memTable.Update("3", []byte{0, 0, 0, 3})
	memTable.Update("1", []byte{0, 0, 0, 1})
	memTable.Update("4", []byte{0, 0, 0, 4})
	memTable.Update("22", []byte{0, 0, 0, 2})

	memTable.Update("33", []byte{0, 0, 0, 3})
	memTable.Update("11", []byte{0, 0, 0, 1})
	memTable.Update("44", []byte{0, 0, 0, 4})

	memTable.Update("111", []byte{0, 0, 0, 10})
	memTable.Update("222", []byte{0, 0, 0, 2})
	memTable.Update("333", []byte{0, 0, 0, 3})
	memTable.Update("444", []byte{0, 0, 0, 1})
	memTable.Update("1111", []byte{0, 0, 0, 4})
	memTable.Update("2222", []byte{0, 0, 0, 10})
	memTable.Update("3333", []byte{0, 0, 0, 2})
	memTable.Update("4444", []byte{0, 0, 0, 3})
	memTable.Update("11111", []byte{0, 0, 0, 1})
	memTable.Update("22222", []byte{0, 0, 0, 4})
	memTable.Update("33333", []byte{0, 0, 0, 10})

	fmt.Println("FLUSH")
	memTable.Flush()

}