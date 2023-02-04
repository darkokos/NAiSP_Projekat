package BTree

type KvPair struct {
	key       []byte
	val       []byte
	tombstone bool
}
type BTreeNode struct {
	parent   *BTreeNode
	children []*BTreeNode
	keys     []KvPair
	d        int
}
type BTree struct {
	root *BTreeNode
	d    int
}

func (t *BTree) Init(d int) {
	(*t).d = d
	(*t).root = nil
}
func (t *BTree) GetValue(key []byte) (int, []byte) {
	if t.root == nil {
		return -1, nil
	}
	ok, node := (*(*t).root).SearchNode(key)
	if ok == -1 {
		return -1, []byte{} //Kljuc nije nadjen
	}
	for _, k := range (*node).keys {
		if string(k.key) == string(key) && k.tombstone == false {
			return 0, k.val
		}
	}
	return -1, []byte{}
}
func (t *BTree) Search(key []byte) (int, *BTreeNode) {
	if t.root == nil {
		return -1, nil
	}
	return (*(*t).root).SearchNode(key)
}
func (n *BTreeNode) SearchNode(key []byte) (int, *BTreeNode) {
	for i, k := range (*n).keys {
		if string(key) == string(k.key) {
			return 0, n
		} else if string(key) < string(k.key) {
			if len((*n).children) == 0 {
				return -1, (*n).parent
			}
			if (*(*n).children[i]).parent != nil && len(n.children[i].keys) > 0 {
				return (*(*n).children[i]).SearchNode(key)
			}
			return -1, n
		}
	}
	if len((*n).children) == 0 {
		return -1, n
	}

	// Ako je trazeni kljuc veci od svih u tom cvoru onda rekurzivno trazimo
	// u poslednjem detetu
	//return n.children[len(n.children) - 1].SearchNode(key)

	if (*(*n).children[len((*n).children)-1]).parent != nil && len(n.children[len(n.children)-1].keys) > 0 {

		return (*(*n).children[len((*n).children)-1]).SearchNode(key)
	}
	return -1, n
}
func (n *BTreeNode) DeleteKey(key []byte) {
	for i, k := range (*n).keys {
		if string(key) == string(k.key) {
			(*n).keys[i].tombstone = true
			return
		} else if string(key) < string(k.key) {
			if len((*n).children) == 0 {
				return
			}
			(*(*n).children[i]).DeleteKey(key)
			return
		}
	}
	if len((*n).children) == 0 {
		return
	}
	(*(*n).children[len((*n).children)-1]).DeleteKey(key)
}
func (t *BTree) Delete(key []byte) {
	(*(*t).root).DeleteKey(key)
}
func (t *BTree) ModifyKey(key []byte, value []byte) int {
	if (*t).root == nil {
		return -1
	}

	ok, node := (*t).Search(key)

	if ok == 0 {
		for i, k := range (*node).keys {
			if string(k.key) == string(key) && k.tombstone == false {
				(*node).keys[i].val = value
			}
		}
		return 0
	}
	return -1
}
func (t *BTree) AddKey(key []byte, value []byte) int {
	pair := KvPair{key: key, val: value, tombstone: false}
	emptyNode1 := &BTreeNode{d: (*t).d}
	emptyNode2 := &BTreeNode{d: (*t).d}
	if (*t).root == nil {
		(*t).root = &BTreeNode{
			keys:     []KvPair{pair},
			d:        (*t).d,
			children: []*BTreeNode{emptyNode1, emptyNode2}}
		return 0
	}

	ok, node := (*t).Search(key)
	// fmt.Print(node, "\n")
	if ok == 0 {
		for i, k := range (*node).keys {
			if string(k.key) == string(key) && k.tombstone == true {
				(*node).keys[i].tombstone = false
				(*node).keys[i].val = value
			}
		}
		return -1 //Kljuc vec postoji, ako nije logicki obrisan, ne radimo nista, ako jeste logicki obrisan treba ga vratiti u opseg, i azurirati vrednost
	}
	//old_len := len(t.GetValuesSortedByKey())
	node.AddKey(pair, t)
	//new_len := len(t.GetValuesSortedByKey())

	/*
		if new_len != old_len+1 {
			fmt.Println()
			fmt.Println("Nestanak elementa kad se dodao kljuc ", string(pair.key))
			panic("Neki elementi su nestali")
		}
	*/
	return 0
}
func (node *BTreeNode) AddKey(pair KvPair, t *BTree) int {
	/*
		if len(node.children) != len(node.keys)+1 {
			fmt.Println()
			fmt.Println(len(node.children), len(node.keys))
			panic("InsertKey preduslovi ne vaze")
		}
	*/
	over, _ := (*node).InsertKey(pair)
	/*
		if len(node.children) != len(node.keys)+1 {
			panic("InsertKey postuslovi ne vaze")
		}
	*/
	emptyNode := &BTreeNode{d: (*node).d}

	var rotationIndex int
	if over == 1 {
		//Premasili smo stepen stabla, treba uraditi rotaciju
		if (*node).parent != nil {
			for i, child := range (*(*node).parent).children {
				if len((*child).keys) < (*node).d && (*child).parent != nil {
					//fmt.Print(*child, "ROTACIJA||\n")
					var x int
					// nasli smo sibling koji ima prostora, rotacija
					for ii, c := range (*(*node).parent).children {
						if c == node {
							x = ii // x je indeks cvora iz koji smo prepunili u nizu dece njegovog roditelja
							break
						}
					}
					if x > i {
						rotationIndex = 0
					} else {
						rotationIndex = (*node).d
					}

					if x-i < -1 || x-i > 1 {
						break // Ne mozemo rotirati, cvor je predaleko (Rotiramo samo izmedju susednih sibling-ova)
					}

					// rotationIndex = x

					/*
						if len(node.parent.children) != len(node.parent.keys)+1 {
							panic("Ovo nije trebalo da se desi 4")
						}
					*/

					if i > 0 {
						if i < x {
							(*child).AddKey((*(*node).parent).keys[i], t)
							(*(*node).parent).keys[i] = (*node).keys[rotationIndex]
						} else {
							(*child).AddKey((*(*node).parent).keys[i-1], t)
							(*(*node).parent).keys[i-1] = (*node).keys[rotationIndex]
						}

						(*node).keys = append((*node).keys[:rotationIndex], (*node).keys[rotationIndex+1:]...)
						(*node).children = append((*node).children[:i], (*node).children[i+1:]...) // Rotacija se radi samo za listove pa ovo nema smisla

						/*
							for j := 0; j < len(node.parent.keys)-1; j++ {
								if string(node.parent.keys[j].key) > string(node.parent.keys[j+1].key) {
									panic("Sortiranost kljuceva ne vazi 1")
								}
							}
						*/
					} else {
						(*child).AddKey((*(*node).parent).keys[i], t) // Ovo izaziva gresku ako je dete koje ima mesta poslednje u nizu dece
						(*(*node).parent).keys[i] = (*node).keys[rotationIndex]

						(*node).keys = append((*node).keys[:rotationIndex], (*node).keys[rotationIndex+1:]...)
						(*node).children = append((*node).children[:i], (*node).children[i+1:]...)

						/*
							for j := 0; j < len(node.parent.keys)-1; j++ {
								if string(node.parent.keys[j].key) > string(node.parent.keys[j+1].key) {
									panic("Sortiranost kljuceva ne vazi 2")
								}
							}
						*/
					}
					// Rotacija se radi samo za listove pa ovo nema smisla

					/*
						if (len(node.children) != len(node.keys)+1) || len(node.children) > t.d+1 {
							panic("Ovo nije trebalo da se desi 3")
						}

						if len(node.parent.children) != len(node.parent.keys)+1 || len(node.parent.keys) > t.d {
							panic("Ovo nije trebalo da se desi 4")
						}

						if len(child.children) != len(child.keys)+1 || len(child.keys) > t.d {
							panic("Ovo nije trebalo da se desi 5")
						}
					*/

					//(*node).AddKey(pair, t) Zbog promene nacina na koji radi insert ovo je sada redundantno
					return 0
				}
			}
		}

		//Ne moze rotacija, ide deljenje cvora
		//fmt.Print("DELJENJE \n")
		newParent := BTreeNode{
			parent: (*node).parent,
			d:      (*node).d}
		parent := &newParent
		if (*node).parent != nil {
			//Deljenje ali ne pravimo novi koren
			parent = (*node).parent

			for parent != nil && len(node.keys) > t.d {
				//old_subtree_len := len(parent.GetValues())

				leftChild := BTreeNode{
					parent: parent,
					d:      (*node).d}
				rightChild := BTreeNode{
					parent: parent,
					d:      (*node).d}

				leftChild.keys = append(leftChild.keys, (*node).keys[:int(len((*node).keys)/2)]...)
				//fmt.Print(leftChild)
				rightChild.keys = append(rightChild.keys, (*node).keys[int(len((*node).keys)/2)+1:]...)

				if len((*node).children) != 0 {
					leftChild.children = append(leftChild.children, (*node).children[:int(len((*node).children)/2)+1]...)
					rightChild.children = append(rightChild.children, (*node).children[int(len((*node).children)/2)+1:]...)

					//fmt.Print("HOPSLA")
					//fmt.Print(int(len((*node).children) / 2))
					//fmt.Print(*rightChild.children[0])

					for _, childOfLeftChild := range leftChild.children {
						if childOfLeftChild != nil {
							childOfLeftChild.parent = &leftChild
						}
					}

					for _, childOfRightChild := range rightChild.children {
						if childOfRightChild != nil {
							childOfRightChild.parent = &rightChild
						}
					}

				}
				parent.InsertKey((*node).keys[int(len((*node).keys)/2)])
				for i, child := range (*parent).children {
					if len(child.keys) == 0 {
						if i != len(parent.keys) {
							parent.children[i] = &leftChild
							parent.children[i+1] = &rightChild
						} else {
							parent.children[i-1] = &leftChild
							parent.children[i] = &rightChild
						}

					}
				}

				/*
					if len(node.children) != len(node.keys)+1 {
						panic("Ovo nije trebalo da se desi")
					}

					if len(parent.children) != len(parent.keys)+1 {
						panic("Ovo nije trebalo da se desi 2")
					}
				*/

				/*
					new_subtree_len := len(parent.GetValues())

					if new_subtree_len != old_subtree_len {
						panic("Promena broja elemenata u deljenju")
					}
				*/

				node = parent
				parent = parent.parent
			}

			if len(node.keys) <= t.d {
				return 0
			} else {
				t.root = &BTreeNode{parent: nil, d: t.d, keys: make([]KvPair, 0), children: make([]*BTreeNode, 2)}
				leftChild := BTreeNode{
					parent: t.root,
					d:      (*node).d}
				rightChild := BTreeNode{
					parent: t.root,
					d:      (*node).d}

				leftChild.keys = append(leftChild.keys, (*node).keys[:int(len((*node).keys)/2)]...)
				rightChild.keys = append(rightChild.keys, (*node).keys[int(len((*node).keys)/2)+1:]...)
				leftChild.children = append(leftChild.children, (*node).children[:int(len((*node).children)/2)+1]...)
				rightChild.children = append(rightChild.children, (*node).children[int(len((*node).children)/2)+1:]...)

				for _, childOfLeftChild := range leftChild.children {
					if childOfLeftChild != nil {
						childOfLeftChild.parent = &leftChild
					}
				}

				for _, childOfRightChild := range rightChild.children {
					if childOfRightChild != nil {
						childOfRightChild.parent = &rightChild
					}
				}

				t.root.keys = append(t.root.keys, node.keys[int(len(node.keys)/2)])
				t.root.children[0] = &leftChild
				t.root.children[1] = &rightChild
				return 0
			}

			//(*parent).AddKey((*node).keys[int(len((*node).keys)/2)], t)
		} else {
			// Pravi se novi koren
			// Pravimo prazan cvor i u njega dodajemo jedan kljuc
			newParent.keys = []KvPair{(*node).keys[int(len((*node).keys)/2)]}
			newParent.children = []*BTreeNode{emptyNode, emptyNode} // Novi koren bi trebalo da ima dvoje dece
		}

		if (*t).root == node {
			(*t).root = &newParent
		}
		leftChild := BTreeNode{
			parent: parent,
			d:      (*node).d}
		rightChild := BTreeNode{
			parent: parent,
			d:      (*node).d}

		leftChild.keys = append(leftChild.keys, (*node).keys[:int(len((*node).keys)/2)]...)
		rightChild.keys = append(rightChild.keys, (*node).keys[int(len((*node).keys)/2)+1:]...)

		if len((*node).children) != 0 {
			leftChild.children = append(leftChild.children, (*node).children[:int(len((*node).children)/2)+1]...)
			rightChild.children = append(rightChild.children, (*node).children[int(len((*node).children)/2)+1:]...)

			//fmt.Print("HOPSLA")
			//fmt.Print(int(len((*node).children) / 2))
			//fmt.Print(*rightChild.children[0])

		}
		// Ovde trazimo mesto za dva nova cvoru u nizu dece
		// InsertKey ce pred deljenje napraviti jedno dete u nizu koje ce biti prazno tj. keys ce mu biti prazan (dodace se onaj emptyNode)
		// Izuzetak je ako se pravi novi koren, tu smo definisali da ce dobiti dvoje dece
		// Na to prazno dete ocito ide levo dete (u korenu ce odmah prvo dete biti prazno)
		// Medjutim to nije slucaj ako smo kljuc dodali na kraj
		// Onda je to zapravo mesto za novo desno dete

		for i, child := range (*parent).children {
			if len(child.keys) == 0 {
				if i != len(parent.keys) {
					parent.children[i] = &leftChild
					parent.children[i+1] = &rightChild
				} else {
					parent.children[i-1] = &leftChild
					parent.children[i] = &rightChild
				}

			}
		}

		/*
			var x int
			for i, child := range (*parent).children {
				if child == node {
					(*parent).children = append((*parent).children[:i], (*parent).children[i+1:]...) // Decu koja su posle, stavi sve pre i
					// kad je i = 1 i d/len(children) = 3 ovo eliminise jedan cvor
					(*parent).children = append((*parent).children[:i+1], (*parent).children[i:]...) // Dva
					(*parent).children = append((*parent).children[:i+1], (*parent).children[i:]...) // ista appenda
					x = i
				}
			}
			if len((*parent).children) != 0 {
				if x == len((*parent).children) {
					(*parent).children = append((*parent).children, &leftChild)
				} else {
					(*parent).children[x] = &leftChild
				}
				if x == len((*parent).children)-1 {
					(*parent).children = append((*parent).children, &rightChild)
				} else {
					(*parent).children[x+1] = &rightChild
					(*parent).children = (*parent).children[:len((*parent).children)-1]
				}
			} else {
				(*parent).children = []*BTreeNode{&leftChild, &rightChild}
			}
			if string(pair.key) < string(leftChild.keys[len(leftChild.keys)-1].key) {
				leftChild.AddKey(pair, t)
				return 0
			}
			rightChild.AddKey(pair, t)
			fmt.Print("right", rightChild)
		*/
	}
	return 0
}
func (node *BTreeNode) InsertKey(pair KvPair) (int, int) {
	over := 0
	emptyNode := &BTreeNode{d: (*node).d}
	if len((*node).keys) == 0 {
		(*node).keys = append((*node).keys, pair)
		(*node).children = append((*node).children, emptyNode)
		return over, 0
	}
	/*
		if len((*node).keys) == 1 {
			if string(pair.key) > string((*node).keys[0].key) {
				(*node).keys = append((*node).keys, pair)
				(*node).children = append((*node).children, emptyNode)
				return over, 1
			} else {
				(*node).keys = append([]KvPair{pair}, (*node).keys...)
				(*node).children = append([]*BTreeNode{emptyNode}, (*node).children...)

				return over, 0
			}
		}
	*/

	//Insert na pocetak ili u sredinu
	for i, _ := range (*node).keys {
		if string(pair.key) < string(node.keys[i].key) {

			node.keys = append(node.keys, KvPair{})
			copy(node.keys[i+1:], node.keys[i:])
			node.keys[i] = pair

			node.children = append(node.children, nil)
			copy(node.children[i+1:], node.children[i:])
			node.children[i] = emptyNode

			if len((*node).keys) == (*node).d+1 {
				over = 1
			}

			return over, i
		}
		/*
			if i == len((*node).keys)-1 {
				// Insert na poslednje mesto, nije izmedju ni jednog
				if len((*node).keys) != (*node).d {
					(*node).keys = append((*node).keys, pair)
					(*node).children = append((*node).children, emptyNode)
				} else {
					over = 1
				}
				return over, i + 1
			}
			if string((*node).keys[i].key) < string(pair.key) && string((*node).keys[i+1].key) > string(pair.key) {
				if len((*node).keys) != (*node).d {
					(*node).keys = append((*node).keys[:i+1], (*node).keys[i:]...)
					(*node).children = append((*node).children[:i+1], (*node).children[i:]...)
					(*node).keys[i] = pair
					(*node).children[i] = emptyNode
				} else {
					over = 1
				}
				return over, i
			}
		*/
	}

	//Insert na poslednje mesto
	(*node).keys = append((*node).keys, pair)              // Da se ne bi kljuc izgubio u deljenju
	(*node).children = append((*node).children, emptyNode) // Deljenje takodje ocekuje dete viska

	if len((*node).keys) == (*node).d+1 {
		over = 1
	}
	return over, len((*node).keys)
}

func (btree *BTree) GetValuesSortedByKey() [][]byte {
	if btree.root != nil {
		return btree.root.GetValues()
	} else {
		return [][]byte{}
	}

}

// Funkcija dobavlja vrednosti sortirane po ključu kojim im je dodeljen
// koristeći in-order prolazak kroz stablo
func (btreeNode *BTreeNode) GetValues() [][]byte {
	values := make([][]byte, 0)

	for i, key_val_pair := range btreeNode.keys {
		if i < len(btreeNode.children) && btreeNode.children[i] != nil {
			for _, value := range btreeNode.children[i].GetValues() {
				values = append(values, value)
			}
		}

		values = append(values, key_val_pair.val)
	}

	if len(btreeNode.children) > 1 && btreeNode.children[len(btreeNode.children)-1] != nil {
		for _, value := range btreeNode.children[len(btreeNode.children)-1].GetValues() {
			values = append(values, value)
		}
	}

	return values

}
