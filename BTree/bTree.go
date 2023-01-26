package BTree
import "fmt"

type BTreeNode struct {
	parent *BTreeNode
	children []*BTreeNode
	keys [][]byte
	toombstone bool
	d int
}
type BTree struct {
	root *BTreeNode
	d int
}
func (t *BTree) Init(d int) {
	(*t).d = d
	(*t).root = nil
}
func (t *BTree) Search(key []byte) (int, *BTreeNode){
	return (*(*t).root).SearchNode(key)
}
func (n *BTreeNode) SearchNode(key []byte) (int, *BTreeNode){
	for i, k := range (*n).keys{
		if (string(key) == string(k)){
			return 0, n
		}else if ( string(key) < string(k)){
			if (len((*n).children) == 0){
				return -1, n
			}
			return (*(*n).children[i]).SearchNode(key)
		}
	}
	if (len((*n).children) == 0){
		return -1, n
	}
	return (*(*n).children[len((*n).children) - 1]).SearchNode(key)
}
func (t *BTree) AddKey(key []byte) int{
	if((*t).root == nil){
		(*t).root = &BTreeNode{
			keys : [][]byte{key},
			d : (*t).d}
		return 0
	}
	fmt.Print("SEARCHING")

	ok, node := (*t).Search(key)
	var rotationIndex int

	if (ok == 0){
		return -1	//Kljuc vec postoji, nema dodavanja
	}
	fmt.Print("INSERTING")
	over, index := (*node).InsertKey(key)
	fmt.Print(over, index)

	if(over == 1){
		//Premasili smo stepen stabla, treba uraditi rotaciju
		for i, child := range (*node).children{
			if (len((*child).keys) < (*t).d){
				//nasli smo sibling koji ima prostora, rotacija
				if (i < index){
					rotationIndex = i
				}else{
					rotationIndex = i - 1
				}
				(*child).InsertKey((*node).keys[rotationIndex])
				(*node).keys[rotationIndex] = key
				return 0
			}
		}

	}
	return 0
}

func (node *BTreeNode) InsertKey(key []byte) (int, int){
	over := 0
	if (len((*node).keys)==0){
		(*node).keys = append((*node).keys, key)
		return over, 0
	}
	if (len((*node).keys)==1){
		if(string(key) > string((*node).keys[0])){
			(*node).keys = append((*node).keys, key)
			return over, 1
		}else{
			(*node).keys = append([][]byte{key}, (*node).keys...)
			return over, 0
		}
	}
	for i, _ := range (*node).keys{
		if ( i ==  len((*node).keys) - 1){
			if(len((*node).keys) != (*node).d){
				(*node).keys = append((*node).keys, key)
			}else{
				over = 1
			}
			return over,i+1
		}
		if (string((*node).keys[i]) < string(key) && string((*node).keys[i+1]) > string(key)){
			if(len((*node).keys) != (*node).d){
				(*node).keys = append((*node).keys[:i+1], (*node).keys[i:]...)
				(*node).keys[i] = key
			}else{
				over = 1
			}
			
			return over,i
		}
	}
	if(len((*node).keys) != (*node).d){
		(*node).keys = append((*node).keys, key)
	}else{
		over = 1
	}
	return over, len((*node).keys)
}