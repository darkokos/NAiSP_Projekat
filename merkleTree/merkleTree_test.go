package merkleTree

import (
	"fmt"
	"testing"
)

func Test(t *testing.T) {
	root := CreateMerkleTree([][]byte{[]byte("BLA"), []byte("BLA"), []byte("BLA"), []byte("BLA"), []byte("BLA")})
	serialized := SerializeTree(root)
	fmt.Print(serialized)
	fmt.Print(len(serialized))
}

func TestOnly2(t *testing.T) {
	root := CreateMerkleTree([][]byte{[]byte("BLA"), []byte("BLA")})
	serialized := SerializeTree(root)
	fmt.Print(serialized)
	fmt.Print(len(serialized))
}
