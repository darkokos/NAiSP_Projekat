package BTree

import "testing"
import "fmt"
func Test(t *testing.T){
	tr := BTree{}
	tr.Init(3)
	tr.AddKey([]byte{1}, []byte {1})
	tr.AddKey([]byte{2}, []byte {2})
	tr.AddKey([]byte{3}, []byte {3})
	tr.AddKey([]byte{4}, []byte {4})
	tr.AddKey([]byte{5}, []byte {5})
	tr.AddKey([]byte{6}, []byte {6})
	
	tr.AddKey([]byte{7}, []byte {7})
	tr.AddKey([]byte{8}, []byte {8})
	tr.AddKey([]byte{9}, []byte {9})

	fmt.Print(tr.GetValue([]byte{1}))
	fmt.Print("\n")
	fmt.Print(tr.GetValue([]byte{2}))
	fmt.Print("\n")
	fmt.Print(tr.GetValue([]byte{3}))
	fmt.Print("\n")
	fmt.Print(tr.GetValue([]byte{4}))
	fmt.Print(tr.GetValue([]byte{5}))
	fmt.Print("6666666666666666")
	fmt.Print(tr.GetValue([]byte{6}))
	fmt.Print("\n")


	fmt.Print(tr.GetValue([]byte{7}))
	fmt.Print(tr.GetValue([]byte{8}))
	fmt.Print(tr.GetValue([]byte{9}))

	fmt.Print("\n")
	fmt.Print("\n")
	fmt.Print("TESTING")
	fmt.Print((*tr.root), "\n")
	fmt.Print(*(*(*tr.root).children[1]).children[0], "BLA\n")
	fmt.Print(*(*tr.root).children[0], "\n")
	fmt.Print(*(*tr.root).children[1], "\n")
	// fmt.Print(*(*tr.root).children[2], "\n")
}