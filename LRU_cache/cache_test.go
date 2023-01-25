package LRU_cache

import "testing"
import "fmt"
func Test(t *testing.T){
	list := Dll{}
	list.Init(5)
	list.Push([]byte("BLA"))
	list.Push([]byte("TA"))
	fmt.Print(list.size)
	list.Push([]byte("DA"))
	
	cache := Cache{}
	cache.Init(10)
	cache.Add([]byte("BLA"), []byte("BLA"))
	cache.Add([]byte("TA"), []byte("TA"))
	fmt.Print(cache.Access([]byte("TA")))
}