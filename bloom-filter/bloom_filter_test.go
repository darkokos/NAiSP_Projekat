package bloomfilter

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"testing"
)


func TestBloomFilter(t *testing.T) {
	fns := CreateHashFunctions(5)

	buf := &bytes.Buffer{}
	encoder := gob.NewEncoder(buf)
	decoder := gob.NewDecoder(buf)

	for _, fn := range fns {
		data := []byte("hello")
		fmt.Println(fn.Hash(data))
		err := encoder.Encode(fn)
		if err != nil {
			panic(err)
		}
		dfn := &HashWithSeed{}
		err = decoder.Decode(dfn)
		if err != nil {
			panic(err)
		}
		fmt.Println(dfn.Hash(data))
	}

	fmt.Println("Penguin Bloom")
	bf := CreateBloomFilter(1024, 3)

	bf.add([]byte("bloomFilter"))
	fmt.Println(bf.find([]byte("bloomFilter")))
	fmt.Println(bf.find([]byte("boomFilter")))

	fmt.Println()

}
