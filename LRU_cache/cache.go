package LRU_cache

type Cache struct {
	capacity int
	size     int
	list     Dll
	hMap     map[string]int //golang mape ne podrzavaju byte arrayeve kao kljuceve, tako da moram da ga konvertujem u string, fuj
}

func (cache *Cache) IsFull() bool {
	return (*cache).list.IsFull()
}
func (cache *Cache) Init(capacity int) {
	(*cache).capacity = capacity
	(*cache).size = 0
	(*cache).hMap = map[string]int{}
	(*cache).list.Init(capacity)
}

// Metoda kojom ce se pristupati elementu sa kljucem u kesu, ako element sa tim kljucem postoji
// Vraca element, i statusnu promenljivu sa vrednosti 0 ili -1, ako je statusna promenljiva -1, treba nastaviti sa read pathom i na kraju dodati element u kes
func (cache *Cache) Access(key []byte) ([]byte, int) {
	el, ok := (*cache).hMap[string(key)]
	if ok {
		return (*cache).list.Get(el), 0
	}
	return []byte{}, -1
}
func (cache *Cache) Add(key []byte, val []byte) {
	for k, _ := range (*cache).hMap {
		(*cache).hMap[k] += 1
	}
	if (*cache).list.IsFull() {
		delete((*cache).hMap, string((*cache).list.GetLast()))
		(*cache).list.DeleteLast()
		(*cache).size--
	}
	(*cache).size++
	(*cache).hMap[string(key)] = 0
	(*cache).list.Push(val)
}

// Postavlja vrednost elementa sa kljucem key na val ako taj element postoji
// U suprotnom ne radi nista
func (cache *Cache) Edit(key []byte, val []byte) {
	cache_index, ok := cache.hMap[string(key)]
	if ok {
		cache.list.Edit(cache_index, val)
	}
}
