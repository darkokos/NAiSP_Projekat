package BTree

import (
	"fmt"
	"testing"
)

func Test(t *testing.T) {
	tr := BTree{}
	tr.Init(3)
	tr.AddKey([]byte{1, 5}, []byte{3})
	//fmt.Print((*tr.root).keys[0])
	tr.AddKey([]byte{1, 1}, []byte{2})
	tr.AddKey([]byte{1, 0}, []byte{1})
	tr.AddKey([]byte{1, 7}, []byte{4})
	fmt.Println()
	tr.GetValuesSortedByKey()
	fmt.Println("Citanje 1-4:", tr.GetValuesSortedByKey())
	fmt.Println()
	tr.AddKey([]byte{1, 9}, []byte{5})
	fmt.Println()
	tr.GetValuesSortedByKey()
	fmt.Println("Citanje 1-5:", tr.GetValuesSortedByKey())
	fmt.Println()
	tr.AddKey([]byte{2, 1}, []byte{6})
	tr.AddKey([]byte{2, 2}, []byte{7}) // Ovaj se pojavljuje 2 puta ???
	tr.AddKey([]byte{2, 3}, []byte{88})
	tr.AddKey([]byte{2, 4}, []byte{9})
	fmt.Println()
	tr.GetValuesSortedByKey()
	fmt.Println("Citanje 1-9:", tr.GetValuesSortedByKey())
	fmt.Println()
	tr.Delete([]byte{2, 3})
	fmt.Print("\n	=============")
	fmt.Print(tr.GetValue([]byte{2, 4}))

	//fmt.Print((*tr.root).keys[1])
	fmt.Print("\n")
	tr.AddKey([]byte{2, 3}, []byte{88})

	//fmt.Print(tr.GetValue([]byte{2, 3}))

	tr.ModifyKey([]byte{2, 3}, []byte{8})
	tr.ModifyKey([]byte{9, 9}, []byte{11}) // Ovaj kljuc ne postoji - ovo ne radi nista

	//fmt.Print(tr.GetValue([]byte{2, 3}))
	//tr.AddKey([]byte{1, 2})
	fmt.Println()
	fmt.Println("Citanje 1-9 (ima jedna modifikovana 11 tamo gde treba da bude 8):", tr.GetValuesSortedByKey())
	fmt.Println()
	fmt.Print("TESTING")
}
