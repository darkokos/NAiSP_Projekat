package merkleTree

var empty []byte
var emptyVal = byte(42)
var emptyNode = Node{}

// func serializeTree(root MerkleRoot) []byte{
// 	var serialized []byte
// 	for i := 0; i < 20; i++ {
// 		empty = append(empty, emptyVal)
// 	}
// 	serializeNode(*root.root, &serialized)
// 	return serialized
// }
// func serializeNode(node Node, serialized *[]byte){
// 	*serialized = append(*serialized, node.data...)

// 	if node.left != nil {
// 		serializeNode(*node.left, serialized)
// 	}else{
// 		*serialized = append(*serialized, empty...)
// 	}

//		if node.right != nil {
//			serializeNode(*node.right, serialized)
//		}else{
//			*serialized = append(*serialized, empty...)
//		}
//	}
func SerializeTree(root MerkleRoot) []byte {
	for i := 0; i < 20; i++ {
		empty = append(empty, emptyVal)
	}
	return serialize([]Node{*root.root}, &[]byte{})
}
func serialize(level []Node, serialized *[]byte) []byte {

	if len(level) == 0 {
		return *serialized
	}
	var nextLevel []Node
	for _, node := range level {
		if node.data == nil {
			*serialized = append(*serialized, empty...)
		} else {
			*serialized = append(*serialized, node.data...)
		}
		if node.left != nil {
			nextLevel = append(nextLevel, *node.left)
		}

		if node.right != nil {
			nextLevel = append(nextLevel, *node.left)
		}
	}
	return serialize(nextLevel, serialized)
}
func DeserializeTree(serializedTree []byte) MerkleRoot {

	rootNode := Node{data: serializedTree[0:20]}
	root := MerkleRoot{root: &rootNode}
	level := []*Node{&rootNode}
	serializedTree = serializedTree[20:]

	for i := 1; len(serializedTree) != 0; i *= 2 {
		level = DeserializeLevel(serializedTree[20:(i*2+1)*20], level)
		serializedTree = serializedTree[(i*2)*20:]
	}
	return root
}
func DeserializeLevel(level []byte, previousLevel []*Node) []*Node {

	var node Node
	var currentLevel []*Node
	for i := 0; i < len(level)/40; i += 40 {
		node.data = level[i : i+20]
		previousLevel[i/20].left = &node
		currentLevel = append(currentLevel, &node)

		node.data = level[i+20 : i+40]
		previousLevel[i/20].right = &node
		currentLevel = append(currentLevel, &node)
	}
	return currentLevel
}
