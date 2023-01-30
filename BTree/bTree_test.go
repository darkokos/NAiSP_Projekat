package BTree

import (
	"fmt"
	"testing"
)

func Test(t *testing.T) {
	tr := BTree{}
	tr.Init(3)
	tr.AddKey([]byte{1, 5}, []byte{1})
	//fmt.Print((*tr.root).keys[0])
	tr.AddKey([]byte{1, 1}, []byte{2})
	tr.AddKey([]byte{1, 0}, []byte{3})
	tr.AddKey([]byte{1, 7}, []byte{4})
	tr.AddKey([]byte{1, 9}, []byte{5})
	tr.AddKey([]byte{2, 1}, []byte{6})
	tr.AddKey([]byte{2, 2}, []byte{7})
	tr.AddKey([]byte{2, 3}, []byte{8})
	tr.AddKey([]byte{2, 4}, []byte{9})
	tr.Delete([]byte{2, 3})
	fmt.Print("\n	=============")
	fmt.Print(tr.GetValue([]byte{2, 4}))

	//fmt.Print((*tr.root).keys[1])
	fmt.Print("\n")
	tr.AddKey([]byte{2, 3}, []byte{7})

	//fmt.Print(tr.GetValue([]byte{2, 3}))

	tr.ModifyKey([]byte{2, 3}, []byte{11})
	tr.ModifyKey([]byte{9, 9}, []byte{11})

	//fmt.Print(tr.GetValue([]byte{2, 3}))
	//tr.AddKey([]byte{1, 2})
	fmt.Print("TESTING")
}

func TestBasedOnMemTable(t *testing.T) {
	tr := BTree{}
	tr.Init(3)
	tr.AddKey([]byte("2"), []byte{0, 0, 0, 2})
	tr.AddKey([]byte("3"), []byte{0, 0, 0, 3})
	tr.AddKey([]byte("1"), []byte{0, 0, 0, 1})
	tr.AddKey([]byte("4"), []byte{0, 0, 0, 4})

	fmt.Println("Dodao 4")

	ok, v := tr.GetValue([]byte("1"))
	if ok == -1 {
		t.Fatalf("Kljuc 1 bi trebalo da postoji")
	} else if v[3] != 1 {
		fmt.Println(v)
		t.Fatalf("Kljuc 1 je nadjen ali vrednost nije dobro iscitana %d", v[3])
	}

	tr.ModifyKey([]byte("1"), []byte{0, 0, 0, 10}) // Menjamo 1
	tr.AddKey([]byte("22"), []byte{0, 0, 0, 2})    // Proizovdi indeks out of range

	ok, v = tr.GetValue([]byte("1"))
	if ok == -1 {
		t.Fatalf("Kljuc 1 bi trebalo da postoji")
	} else if v[3] != 10 {
		fmt.Println(v)
		t.Fatalf("Kljuc 1 je nadjen ali vrednost nije dobro iscitana %d", v[3])
	}

	fmt.Print("TESTING")
}
