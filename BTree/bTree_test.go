package BTree

import "testing"
import "fmt"
func Test(t *testing.T){
	tr := BTree{}
	tr.Init(3)
	tr.AddKey([]byte{1, 5}, []byte {1})
	//fmt.Print((*tr.root).keys[0])
	tr.AddKey([]byte{1, 1}, []byte {2})
	tr.AddKey([]byte{1, 0}, []byte {3})
	tr.AddKey([]byte{1, 7}, []byte {4})
	tr.AddKey([]byte{1, 9}, []byte {5})
	tr.AddKey([]byte{2, 1}, []byte {6})
	tr.AddKey([]byte{2, 2}, []byte {7})
	tr.AddKey([]byte{2, 3}, []byte {8})
	tr.AddKey([]byte{2, 4}, []byte {9})
	tr.Delete([]byte{2, 3})
	fmt.Print("\n	=============")
	fmt.Print(tr.GetValue([]byte{2, 4}))


	//fmt.Print((*tr.root).keys[1])
	fmt.Print("\n")
	tr.AddKey([]byte{2, 3}, []byte {7})

	//fmt.Print(tr.GetValue([]byte{2, 3}))

	tr.ModifyKey([]byte{2, 3}, []byte {11})
	tr.ModifyKey([]byte{9, 9}, []byte {11})

	//fmt.Print(tr.GetValue([]byte{2, 3}))
	//tr.AddKey([]byte{1, 2})
	fmt.Print("TESTING")
}