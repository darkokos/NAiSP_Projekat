package merkleTree

import (
	"fmt"
	"testing"
)

func Test(t *testing.T) {
	root := CreateMerkleTree([][]byte{[]byte("BLA"), []byte("BLA"), []byte("BLA"), []byte("BLA"), []byte("BLA")})
	serialized := serializeTree(root)
	fmt.Print(serialized)
	fmt.Print(len(serialized))
}
