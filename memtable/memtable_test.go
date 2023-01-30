package memtable

import (
	"fmt"
	"testing"
)

func TestMemtable(t *testing.T) {
	memTable := MakeHashMapMemTable(5)

	memTable.Update("2", []byte{0, 0, 0, 2})
	memTable.Update("3", []byte{0, 0, 0, 3})
	memTable.Update("1", []byte{0, 0, 0, 1})
	memTable.Update("4", []byte{0, 0, 0, 4})
	memTable.Update("1", []byte{0, 0, 0, 10})

	v, ok := memTable.Get("1")
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

	memTable.Update("5", []byte{0, 0, 0, 5})

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

	_, ok = memTable.Get("5")
	if ok {
		t.Fatalf("Memtable bi trebalo da je bio flush-ovan")
	}
}
