package skiplist

import "testing"

func TestSkipList(t *testing.T) {

	list := NewSkipList(5)
	list.Insert("Gojko", []byte("Vuckovic"))
	list.Insert("Darko", []byte("Svilar"))
	list.Insert("Marko", []byte("Kuvizic"))
	list.Insert("Momir", []byte("Milutnovic"))
	list.Insert("Vuk", []byte("Dimitrov"))

	list.Insert("1", []byte("Svilar"))
	list.Insert("2", []byte("Vuckovic"))
	list.Insert("3", []byte("Kuvizic"))
	list.Insert("4", []byte("Milutnovic"))
	list.Insert("5", []byte("Dimitrov"))

	list.Insert("11", []byte("Svilar"))
	list.Insert("22", []byte("Vuckovic"))
	list.Insert("33", []byte("Kuvizic"))
	list.Insert("44", []byte("Milutnovic"))
	list.Insert("55", []byte("Dimitrov"))

	list.PrintList()
}
