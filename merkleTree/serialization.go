package merkleTree

<<<<<<< HEAD
var empty []byte
var emptyVal = byte(42)

func SerializeTree(root MerkleRoot) []byte {
=======

var empty []byte
var emptyVal = byte(42)

func serializeTree(root MerkleRoot) []byte{
>>>>>>> 0682d82 (Implementacija Merkle Stabla)
	var serialized []byte
	for i := 0; i < 20; i++ {
		empty = append(empty, emptyVal)
	}
	serializeNode(*root.root, &serialized)
	return serialized
}
<<<<<<< HEAD
func serializeNode(node Node, serialized *[]byte) {
=======
func serializeNode(node Node, serialized *[]byte){
>>>>>>> 0682d82 (Implementacija Merkle Stabla)
	*serialized = append(*serialized, node.data...)

	if node.left != nil {
		serializeNode(*node.left, serialized)
<<<<<<< HEAD
	} else {
=======
	}else{
>>>>>>> 0682d82 (Implementacija Merkle Stabla)
		*serialized = append(*serialized, empty...)
	}

	if node.right != nil {
		serializeNode(*node.right, serialized)
<<<<<<< HEAD
	} else {
		*serialized = append(*serialized, empty...)
	}
}
=======
	}else{
		*serialized = append(*serialized, empty...)
	}
}

>>>>>>> 0682d82 (Implementacija Merkle Stabla)
