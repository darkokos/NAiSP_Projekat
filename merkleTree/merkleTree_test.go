package merkleTree
import "testing"
import "fmt"
func Test(t *testing.T){
	root := CreateMerkleTree([][]byte{[]byte("BLA"), []byte("BLA"), []byte("BLA"), []byte("BLA"), []byte("BLA")})
	fmt.Print((*root.root).data, "\n")
	serialized := SerializeTree(root)
	fmt.Print(serialized)
	fmt.Print(len(serialized), "\n")
	deserialized := DeserializeTree(serialized)
	fmt.Print((*deserialized.root).data, "\n")
	fmt.Print(SerializeTree(deserialized), "\n")

}
