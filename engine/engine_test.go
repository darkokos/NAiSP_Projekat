package engine

import (
	"fmt"
	"testing"
)

func TestEngineSimple(t *testing.T) {
	db := GetNewDB()

	for i := 0; i < 100; i++ {
		ok := db.Put(fmt.Sprintf("%03d", i), []byte{uint8(i)})
		if !ok {
			t.Fatalf("Nesto je poslo po zlu")
		}
	}

}
