package BTree

import "testing"
import "fmt"
func Test(t *testing.T){
	tr := BTree{}
	tr.Init(3)
	tr.AddKey([]byte{1, 5})
	fmt.Print((*tr.root).keys[0])
	tr.AddKey([]byte{1, 1})
	tr.AddKey([]byte{1, 0})
	tr.AddKey([]byte{1, 7})
	fmt.Print((*tr.root).keys[0])
	fmt.Print((*tr.root).keys[1])


	//tr.AddKey([]byte{1, 2})
	fmt.Print("TESTING")
}