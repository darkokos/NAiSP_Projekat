package LRU_cache

import (
	"fmt"
	"testing"
)

func Test(t *testing.T) {
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

func TestReadOne(t *testing.T) {
	// Testira citanje iz kesa koji ima samo jedan element
	cache := Cache{}
	cache.Init(10)
	cache.Add([]byte("Only"), []byte{1})

	v, ok := cache.Access([]byte("Only"))
	if ok != 0 {
		t.Fatalf("Trebalo je da nadje ovo")
	} else if v[0] != 1 {
		t.Fatalf("Nije dobro procitana vrednost")
	}
}
