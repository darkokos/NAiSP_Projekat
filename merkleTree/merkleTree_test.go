package merkleTree

import "testing"
import "fmt"

func Test(t *testing.T) {
	root := CreateMerkleTree([][]byte{[]byte("BLA"), []byte("BLA"), []byte("BLA"), []byte("BLA"), []byte("BLA")})
	serialized := SerializeTree(root)
	fmt.Print(serialized)
	fmt.Print(len(serialized), "\n")
	deserialized := DeserializeTree(serialized)
	fmt.Print(SerializeTree(deserialized), "\n")

}
