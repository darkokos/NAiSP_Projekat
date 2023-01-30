package merkleTree


var hashSize = 20	//Velicina jednog zapisa, u vidu broja bajtova, u specifikaciji je 20
//Funkcija radi serijalizaciju stabla iz korena
func SerializeTree(root MerkleRoot) []byte{
	
	return serialize([]Node{*root.root}, &[]byte{})
}
func serialize(level []Node, serialized *[]byte) []byte{
	
	if(len(level) == 0){
		return *serialized
	}
	var nextLevel []Node
	for _, node := range level{
		if node.data != nil{	//Common sense provera, verovatno nije potrebna
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
//Deserijalizacija stabla na osnovu niza bajtova
func DeserializeTree(serializedTree []byte) MerkleRoot{
	
	rootNode := Node{ data:serializedTree[0:hashSize] }	//Konstrukcija korena
	root := MerkleRoot{ root:&rootNode}
	level := [] * Node {&rootNode}	//Posto je serijalizacija uradjena bfs prolazom, niz bajtova delimo na nivoe stabla
	serializedTree = serializedTree[hashSize:]
	
	for i := 1; len(serializedTree) != 0; i*=2{
		level = DeserializeLevel(serializedTree[hashSize:(i*2+1)*hashSize], level)	//izdvajanje jednog nivoa
		serializedTree = serializedTree[(i*2)*hashSize:]	//popovanje tog nivoa iz niza
	}
	return root
}
//Funkcija deserijalizuje jedan nivo stabla
func DeserializeLevel(level []byte, previousLevel []*Node) []*Node{
	
	var node Node
	var currentLevel []*Node
	for i := 0; i < len(level)/(2*hashSize); i+=(2*hashSize){	//Svaka dva zapisa u nivou su redom levo i desno dete odgovarajuceg cvora u nivou iznad
		node.data = level[i:i+hashSize]
		previousLevel[i/hashSize].left = &node
		currentLevel = append(currentLevel, &node)

		node.data = level[i+hashSize:i+(2*hashSize)]
		previousLevel[i/hashSize].right = &node
		currentLevel = append(currentLevel, &node)
	}
	return currentLevel
}