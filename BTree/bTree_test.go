package BTree

import (
	"fmt"
	"testing"
)

func Test(t *testing.T) {
	tr := BTree{}
	tr.Init(3)
	tr.AddKey([]byte{1, 5}, []byte{6})
	fmt.Print((*tr.root).keys[0])
	tr.AddKey([]byte{1, 1}, []byte{6})
	tr.AddKey([]byte{1, 0}, []byte{6})

	ok, v := tr.GetValue([]byte{1, 1})

	if ok == -1 {
		t.Fatalf("Trebalo je da nadje ovo")
	}
	if len(v) < 1 || v[0] != 6 {
		t.Fatalf("Nesto nije uredu sa nadjenom vrednoscu")
	}

	tr.AddKey([]byte{1, 7}, []byte{6})
	tr.AddKey([]byte{1, 9}, []byte{6})
	tr.AddKey([]byte{2, 1}, []byte{6})
	tr.AddKey([]byte{2, 2}, []byte{6})
	tr.AddKey([]byte{2, 3}, []byte{6})
	tr.AddKey([]byte{2, 4}, []byte{10})
	tr.Delete([]byte{2, 3})
	fmt.Print("\n")
	fmt.Print(tr.GetValue([]byte{2, 4}))

	ok, v = tr.GetValue([]byte{2, 4}) // Bio je dodat u liniji 30

	if ok == -1 {
		// Ovde FAIL
		t.Fatalf("Trebalo je da nadje ovo")
	}
	if len(v) < 1 || v[0] != 10 {
		t.Fatalf("Nesto nije uredu sa nadjenom vrednoscu")
	}

	ok, _ = tr.GetValue([]byte{5, 5})

	if ok != -1 {
		t.Fatalf("Nije trebalo je da nadje ovo")
	}

	fmt.Print((*tr.root).keys[1])
	fmt.Print("\n")
	tr.AddKey([]byte{2, 3}, []byte{7})

	fmt.Print(tr.GetValue([]byte{2, 3}))

	tr.ModifyKey([]byte{2, 3}, []byte{11})
	tr.ModifyKey([]byte{9, 9}, []byte{11})

	fmt.Print(tr.GetValue([]byte{2, 3}))

	ok, v = tr.GetValue([]byte{2, 3})

	if ok == -1 {
		t.Fatalf("Trebalo je da nadje ovo")
	}
	if len(v) < 1 || v[0] != 11 {
		t.Fatalf("Nesto nije uredu sa nadjenom vrednoscu")
	}

	//tr.AddKey([]byte{1, 2})
	fmt.Print("TESTING")
}

func TestBasedOnMemtable(t *testing.T) {
	tr := BTree{}
	tr.Init(3)
	tr.AddKey([]byte("2"), []byte{6})
	tr.AddKey([]byte("3"), []byte{6})
	tr.AddKey([]byte("4"), []byte{6})
	tr.AddKey([]byte("1"), []byte{5})

	ok, v := tr.GetValue([]byte("1"))

	if ok == -1 {
		t.Fatalf("Trebalo je da nadje 1")
	} else if v[0] != 5 {
		t.Fatalf("Nesto nije uredu sa vracenom vrednoscu")
	}

}
