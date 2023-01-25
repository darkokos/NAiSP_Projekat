package memtable

import (
	"fmt"
	"testing"
)

func TestMemtable(t *testing.T) {
	fmt.Println("Kompajlirao se!")

	//memTable := createMemTableFromConfig()

	memTable := makeHashMapMemTable(5)
	//fmt.Println(memTable.data)

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

	_, ok = memTable.Get("5")
	if ok {
		t.Fatalf("Kljuc 5 nadjen pre nego sto je dodat")
	}

	memTable.Update("5", []byte{0, 0, 0, 5})

	v, ok = memTable.Get("2")
	if ok {
		t.Fatalf("Memtable bi trebalo da je bio flush-ovan")
	}

	fmt.Println("Flush2")
	memTable.Update("5", []byte{0, 0, 0, 2})
	memTable.Update("36", []byte{0, 0, 0, 3})
	memTable.Update("231", []byte{0, 0, 0, 1})
	memTable.Update("33", []byte{0, 0, 0, 4})

	ok = memTable.Delete("37")
	if ok {
		t.Fatalf("Brisanje nepostojeceg kljuca ne bi trebalo da bude uspesno")
	}
	fmt.Println("Brisanje neuspesno", ok)

	ok = memTable.Delete("33")
	if !ok {
		t.Fatalf("Brisanje postojeceg kljuca bi trebalo da bude uspesno")
	}
	fmt.Println("Brisanje uspesno", ok)

	//v, ok = memTable.Get("33")
	/*if !ok {
		t.Fatalf("Kljuc 33 bi trebalo da je i dalje tu")
	} else if !v.tombstone {
		t.Fatalf("Kljuc 33 bi trebalo da ima postavljen tombstone")
	}*/

	memTable.Update("11", []byte{0, 0, 0, 10})
	//memTable.Update("5", []byte{0,0,0,5})

	_, ok = memTable.Get("5")
	if ok {
		t.Fatalf("Memtable bi trebalo da je bio flush-ovan")
	}
}
