package BTree
import "fmt"
type KvPair struct {
	key [] byte
	val [] byte
	tombstone bool
}
type BTreeNode struct {
	parent *BTreeNode
	children []*BTreeNode
	keys []KvPair
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
func (t *BTree) GetValue(key []byte) (int, []byte){
	ok, node := (*(*t).root).SearchNode(key)
	if(ok == -1){
		return -1, []byte{}	//Kljuc nije nadjen
	}
	for _, k := range (*node).keys{
		if (string(k.key) == string(key) && k.tombstone == false){
			return 0, k.val
		}
	}
	return -1, []byte{}
}
func (t *BTree) Search(key []byte) (int, *BTreeNode){
	return (*(*t).root).SearchNode(key)
}
func (n *BTreeNode) SearchNode(key []byte) (int, *BTreeNode){
	fmt.Print(*n, "\n")
	for i, k := range (*n).keys{
		if (string(key) == string(k.key)){
			return 0, n
		}else if ( string(key) < string(k.key)){
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
func (n *BTreeNode) DeleteKey(key []byte){
	for i, k := range (*n).keys{
		if (string(key) == string(k.key)){
			(*n).keys[i].tombstone = true
			return 
		}else if ( string(key) < string(k.key)){
			if (len((*n).children) == 0){
				return 
			}
			(*(*n).children[i]).DeleteKey(key)
			return
		}
	}
	if (len((*n).children) == 0){
		return
	}
	(*(*n).children[len((*n).children) - 1]).DeleteKey(key)
} 
func (t *BTree) Delete(key []byte) {
	(*(*t).root).DeleteKey(key)
}
func (t *BTree) ModifyKey(key []byte, value []byte) int {
	if((*t).root == nil){
		return -1
	}

	ok, node := (*t).Search(key)

	if (ok == 0){
		for i, k := range (*node).keys{
			if(string(k.key) == string(key) && k.tombstone == false){
				(*node).keys[i].val = value
			}
		}
		return 0	
	}
	return -1
}
func (t *BTree) AddKey(key []byte, value []byte) int{
	pair := KvPair {key : key, val : value, tombstone : false}
	if((*t).root == nil){
		(*t).root = &BTreeNode{
			keys : []KvPair{pair},
			d : (*t).d}
		return 0
	}

	ok, node := (*t).Search(key)

	if (ok == 0){
		for i, k := range (*node).keys{
			if(string(k.key) == string(key) && k.tombstone == true){
				(*node).keys[i].tombstone = false
				(*node).keys[i].val = value
			}
		}
		return -1	//Kljuc vec postoji, ako nije logicki obrisan, ne radimo nista, ako jeste logicki obrisan treba ga vratiti u opseg, i azurirati vrednost
	}
	node.AddKey(pair, t)
	return 0
}
func (node *BTreeNode) AddKey(pair KvPair, t *BTree) int{
	over, index := (*node).InsertKey(pair)
	var rotationIndex int
	if(over == 1){
		//Premasili smo stepen stabla, treba uraditi rotaciju
		for i, child := range (*node).children{
			if (len((*child).keys) < (*node).d){
				//nasli smo sibling koji ima prostora, rotacija
				if (i < index){
					rotationIndex = i
				}else{
					rotationIndex = i - 1
				}
				(*child).InsertKey((*node).keys[rotationIndex])
				(*node).keys[rotationIndex] = pair
				return 0
			}
		}
		//Ne moze rotacija, ide deljenje cvora
		newParent := BTreeNode{
			parent : (*node).parent,
			d : (*node).d}
		parent := &newParent
		if ((*node).parent != nil){
			parent = (*node).parent
			(*parent).AddKey((*node).keys[int(len((*node).keys)/2)], t)
		}else{
			newParent.keys = []KvPair{(*node).keys[int(len((*node).keys)/2)]}
		}
		if((*t).root == node){
			(*t).root = &newParent
		}
		leftChild := BTreeNode{
			parent : parent,
			d : (*node).d}	
		rightChild := BTreeNode{
			parent : parent,
			d : (*node).d}	
		leftChild.keys = (*node).keys[:int(len((*node).keys)/2)]
		rightChild.keys = (*node).keys[int(len((*node).keys)/2)+1:]
		if(len ((*node).children) != 0){
			leftChild.children = (*node).children[:int(len((*node).children)/2)]
			rightChild.children = (*node).children[int(len((*node).children)/2)+1:]
		}
		
		if(string(pair.key) < string(leftChild.keys[len(leftChild.keys)-1].key)){
			leftChild.AddKey(pair, t)
			return 0
		}
		rightChild.AddKey(pair, t)
		(*parent).children = []*BTreeNode{&leftChild, &rightChild}
	}
	return 0
}
func (node *BTreeNode) InsertKey(pair KvPair) (int, int){
	over := 0
	emptyNode := &BTreeNode{d : (*node).d}
	if (len((*node).keys)==0){
		(*node).keys = append((*node).keys, pair)
		(*node).children = append((*node).children, emptyNode)
		return over, 0
	}
	if (len((*node).keys)==1){
		if(string(pair.key) > string((*node).keys[0].key)){
			(*node).keys = append((*node).keys, pair)
			(*node).children = append((*node).children, emptyNode)
			return over, 1
		}else{
			(*node).keys = append([]KvPair{pair}, (*node).keys...)
			(*node).children = append([]*BTreeNode{emptyNode}, (*node).children...)

			return over, 0
		}
	}
	for i, _ := range (*node).keys{
		if ( i ==  len((*node).keys) - 1){
			if(len((*node).keys) != (*node).d){
				(*node).keys = append((*node).keys, pair)
			}else{
				over = 1
			}
			return over,i+1
		}
		if (string((*node).keys[i].key) < string(pair.key) && string((*node).keys[i+1].key) > string(pair.key)){
			if(len((*node).keys) != (*node).d){
				(*node).keys = append((*node).keys[:i+1], (*node).keys[i:]...)
				(*node).children = append((*node).children[:i+1], (*node).children[i:]...)
				(*node).keys[i] = pair
				(*node).children[i] = emptyNode
			}else{
				over = 1
			}
			
			return over,i
		}
	}
	if(len((*node).keys) != (*node).d){
		(*node).keys = append((*node).keys, pair)
	}else{
		over = 1
	}
	return over, len((*node).keys)
}