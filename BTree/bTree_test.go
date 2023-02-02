package BTree

import (
	"fmt"
	"testing"
)

func Test(t *testing.T) {
	tr := BTree{}
	tr.Init(3)
	tr.AddKey([]byte{1, 5}, []byte{3})
	//fmt.Print((*tr.root).keys[0])
	tr.AddKey([]byte{1, 1}, []byte{2})
	tr.AddKey([]byte{1, 0}, []byte{1})
	tr.AddKey([]byte{1, 7}, []byte{4})
	fmt.Println()
	tr.GetValuesSortedByKey()
	fmt.Println("Citanje 1-4:", tr.GetValuesSortedByKey())
	fmt.Println()
	tr.AddKey([]byte{1, 9}, []byte{5})
	fmt.Println()
	tr.GetValuesSortedByKey()
	fmt.Println("Citanje 1-5:", tr.GetValuesSortedByKey())
	fmt.Println()
	tr.AddKey([]byte{2, 1}, []byte{6})
	tr.AddKey([]byte{2, 2}, []byte{7}) // Ovaj se pojavljuje 2 puta ???
	tr.AddKey([]byte{2, 3}, []byte{88})
	tr.AddKey([]byte{2, 4}, []byte{9})
	fmt.Println()
	tr.GetValuesSortedByKey()
	fmt.Println("Citanje 1-9:", tr.GetValuesSortedByKey())
	fmt.Println()
	tr.Delete([]byte{2, 3})
	fmt.Print("\n	=============")
	fmt.Print(tr.GetValue([]byte{2, 4}))

	//fmt.Print((*tr.root).keys[1])
	fmt.Print("\n")
	tr.AddKey([]byte{2, 3}, []byte{88})

	//fmt.Print(tr.GetValue([]byte{2, 3}))

	tr.ModifyKey([]byte{2, 3}, []byte{8})
	tr.ModifyKey([]byte{9, 9}, []byte{11}) // Ovaj kljuc ne postoji - ovo ne radi nista

	//fmt.Print(tr.GetValue([]byte{2, 3}))
	//tr.AddKey([]byte{1, 2})
	fmt.Println()
	fmt.Println("Citanje 1-9 (ima jedna modifikovana 11 tamo gde treba da bude 8):", tr.GetValuesSortedByKey())
	fmt.Println()
	fmt.Print("TESTING")
}

func TestRotation(t *testing.T) {
	tr := BTree{}
	tr.Init(3)

	tr.root = &BTreeNode{
		d:        3,
		parent:   nil,
		children: make([]*BTreeNode, 0),
	}

	tr.root.InsertKey(KvPair{key: []byte("030"), val: nil})
	tr.root.InsertKey(KvPair{key: []byte("060"), val: nil})
	tr.root.children = append(tr.root.children, &BTreeNode{d: 3, parent: tr.root})
	tr.root.children[0].parent = tr.root
	tr.root.children[0].children = append(tr.root.children[0].children, &BTreeNode{parent: tr.root.children[0], d: 3})
	tr.root.children[1].parent = tr.root
	tr.root.children[1].children = append(tr.root.children[1].children, &BTreeNode{parent: tr.root.children[1], d: 3})
	tr.root.children[2].parent = tr.root
	tr.root.children[2].children = append(tr.root.children[2].children, &BTreeNode{parent: tr.root.children[2], d: 3})

	tr.root.children[0].InsertKey(KvPair{key: []byte("000"), val: nil})
	tr.root.children[0].InsertKey(KvPair{key: []byte("010"), val: nil})
	tr.root.children[0].InsertKey(KvPair{key: []byte("020"), val: nil})

	tr.root.children[1].InsertKey(KvPair{key: []byte("040"), val: nil})
	tr.root.children[1].InsertKey(KvPair{key: []byte("050"), val: nil})

	tr.root.children[2].InsertKey(KvPair{key: []byte("070"), val: nil})
	//tr.root.InsertKey()

	tr.AddKey([]byte("021"), nil)

	if string(tr.root.keys[0].key) != "021" {
		fmt.Println(string(tr.root.keys[0].key))
		t.Fatalf("Nije se dobro desila rotacija")
	}

	if string(tr.root.children[1].keys[0].key) != "030" {
		fmt.Println(string(tr.root.children[1].keys[2].key))
		t.Fatalf("Nije se dobro desila rotacija")
	}

	//tr.AddKey([]byte{"00"}, []byte{})

}

func TestRotation2(t *testing.T) {
	tr := BTree{}
	tr.Init(3)

	tr.root = &BTreeNode{
		d:        3,
		parent:   nil,
		children: make([]*BTreeNode, 0),
	}

	tr.root.InsertKey(KvPair{key: []byte("030"), val: nil})
	tr.root.InsertKey(KvPair{key: []byte("060"), val: nil})
	tr.root.children = append(tr.root.children, &BTreeNode{d: 3, parent: tr.root})
	tr.root.children[0].parent = tr.root
	tr.root.children[0].children = append(tr.root.children[0].children, &BTreeNode{parent: tr.root.children[0], d: 3})
	tr.root.children[1].parent = tr.root
	tr.root.children[1].children = append(tr.root.children[1].children, &BTreeNode{parent: tr.root.children[1], d: 3})
	tr.root.children[2].parent = tr.root
	tr.root.children[2].children = append(tr.root.children[2].children, &BTreeNode{parent: tr.root.children[2], d: 3})

	tr.root.children[0].InsertKey(KvPair{key: []byte("000"), val: nil})
	tr.root.children[0].InsertKey(KvPair{key: []byte("010"), val: nil})
	//tr.root.children[0].InsertKey(KvPair{key: []byte("020"), val: nil})

	tr.root.children[1].InsertKey(KvPair{key: []byte("040"), val: nil})
	tr.root.children[1].InsertKey(KvPair{key: []byte("050"), val: nil})
	tr.root.children[1].InsertKey(KvPair{key: []byte("055"), val: nil})

	tr.root.children[2].InsertKey(KvPair{key: []byte("070"), val: nil})
	tr.root.children[2].InsertKey(KvPair{key: []byte("071"), val: nil})
	tr.root.children[2].InsertKey(KvPair{key: []byte("072"), val: nil})
	//tr.root.InsertKey()

	tr.AddKey([]byte("056"), nil)

	if string(tr.root.keys[0].key) != "040" {
		fmt.Println(string(tr.root.keys[0].key))
		t.Fatalf("Nije se dobro desila rotacija")
	}

	if string(tr.root.children[1].keys[2].key) != "056" {
		fmt.Println(string(tr.root.children[1].keys[2].key))
		t.Fatalf("Nije se dobro desila rotacija")
	}

	//tr.AddKey([]byte{"00"}, []byte{})
}

func TestRotation3(t *testing.T) {
	tr := BTree{}
	tr.Init(3)

	tr.root = &BTreeNode{
		d:        3,
		parent:   nil,
		children: make([]*BTreeNode, 0),
	}

	tr.root.InsertKey(KvPair{key: []byte("030"), val: nil})
	tr.root.InsertKey(KvPair{key: []byte("060"), val: nil})
	tr.root.children = append(tr.root.children, &BTreeNode{d: 3, parent: tr.root})
	tr.root.children[0].parent = tr.root
	tr.root.children[0].children = append(tr.root.children[0].children, &BTreeNode{parent: tr.root.children[0], d: 3})
	tr.root.children[1].parent = tr.root
	tr.root.children[1].children = append(tr.root.children[1].children, &BTreeNode{parent: tr.root.children[1], d: 3})
	tr.root.children[2].parent = tr.root
	tr.root.children[2].children = append(tr.root.children[2].children, &BTreeNode{parent: tr.root.children[2], d: 3})

	tr.root.children[0].InsertKey(KvPair{key: []byte("000"), val: nil})
	tr.root.children[0].InsertKey(KvPair{key: []byte("010"), val: nil})
	tr.root.children[0].InsertKey(KvPair{key: []byte("020"), val: nil})

	tr.root.children[1].InsertKey(KvPair{key: []byte("040"), val: nil})
	tr.root.children[1].InsertKey(KvPair{key: []byte("050"), val: nil})
	tr.root.children[1].InsertKey(KvPair{key: []byte("055"), val: nil})

	tr.root.children[2].InsertKey(KvPair{key: []byte("070"), val: nil})
	tr.root.children[2].InsertKey(KvPair{key: []byte("071"), val: nil})
	//tr.root.children[2].InsertKey(KvPair{key: []byte("072"), val: nil})
	//tr.root.InsertKey()

	tr.AddKey([]byte("056"), nil)

	if string(tr.root.keys[1].key) != "056" {
		fmt.Println(string(tr.root.keys[1].key))
		t.Fatalf("Nije se dobro desila rotacija")
	}

	if string(tr.root.children[2].keys[0].key) != "060" {
		fmt.Println(string(tr.root.children[2].keys[0].key))
		t.Fatalf("Nije se dobro desila rotacija")
	}

	//tr.AddKey([]byte{"00"}, []byte{})

}
