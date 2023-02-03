package merkleTree

//Implementacija Merkle Stabla

type MerkleRoot struct {
	root *Node
}

// Jedan cvor
type Node struct {
	data  []byte
	left  *Node
	right *Node
}

// Funkcija konstruise merkle stablo sa elementima elems
// Jedan element je byte array, primamo array elemenata
func CreateMerkleTree(elems [][]byte) MerkleRoot {

	return BuildRow(DataToLeafNodes(elems))
}

// funkcija gradi cvor koji ce biti roditelj prosledjenim cvorovima
func BuildNode(left Node, right Node) Node {
	hash := Hash(append(left.data, right.data...))
	return Node{
		left:  &left,
		right: &right,
		data:  hash[:]} //[:] je array to slice konverzija, voleo bih da nadjem elegantnije resenje
}

// funkcija gradi sledeci red stabla
func BuildRow(nodes []Node) MerkleRoot {
	var row []Node
	//Ako imamo neparan broj elemenata, poslednji se ponavlja
	if len(nodes)%2 == 1 {
		nodes = append(nodes, nodes[len(nodes)-1])
	}
	for i := 0; i < len(nodes); i += 2 {
		row = append(row, BuildNode(nodes[i], nodes[i+1]))
	}

	if len(row) == 1 {
		return MerkleRoot{root: &row[0]}
	}
	if len(row) != 2 {
		BuildRow(row) //Funkcija se rekurzivno poziva dok ne dodjemo do poslednjeg reda
	}
	r := BuildNode(row[0], row[1]) //Konstrukcija korena
	return MerkleRoot{root: &r}
}

// funkcija konvertuje niz podataka u cvorove stabla (poziva se za prvi red koji ce postati listovi stabla)
func DataToLeafNodes(elems [][]byte) []Node {
	var leafs []Node
	var hash [20]byte
	for _, element := range elems {
		hash = Hash(element)
		leafs = append(leafs, Node{data: hash[:]})
	}

	return leafs
}

func (mr *MerkleRoot) String() string {
	return mr.root.String()
}
