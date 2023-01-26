package BTree

import "testing"
import "fmt"
func Test(t *testing.T){
	tr := BTree{}
	tr.Init(3)
	tr.AddKey([]byte{1, 5}, []byte {6})
	fmt.Print((*tr.root).keys[0])
	tr.AddKey([]byte{1, 1}, []byte {6})
	tr.AddKey([]byte{1, 0}, []byte {6})
	tr.AddKey([]byte{1, 7}, []byte {6})
	tr.AddKey([]byte{1, 9}, []byte {6})
	tr.AddKey([]byte{2, 1}, []byte {6})
	tr.AddKey([]byte{2, 2}, []byte {6})
	tr.AddKey([]byte{2, 3}, []byte {6})
	tr.AddKey([]byte{2, 4}, []byte {10})
	tr.Delete([]byte{2, 3})
	fmt.Print("\n")
	fmt.Print(tr.GetValue([]byte{2, 4}))


	fmt.Print((*tr.root).keys[1])
	fmt.Print("\n")
	tr.AddKey([]byte{2, 3}, []byte {7})

	fmt.Print(tr.GetValue([]byte{2, 3}))


	//tr.AddKey([]byte{1, 2})
	fmt.Print("TESTING")
}