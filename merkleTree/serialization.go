package merkleTree

var empty []byte
var emptyVal = byte(42)

func SerializeTree(root MerkleRoot) []byte {
	var serialized []byte
	for i := 0; i < 20; i++ {
		empty = append(empty, emptyVal)
	}
	serializeNode(*root.root, &serialized)
	return serialized
}
func serializeNode(node Node, serialized *[]byte) {
	*serialized = append(*serialized, node.data...)

	if node.left != nil {
		serializeNode(*node.left, serialized)
	} else {
		*serialized = append(*serialized, empty...)
	}

	if node.right != nil {
		serializeNode(*node.right, serialized)
	} else {
		*serialized = append(*serialized, empty...)
	}
}
