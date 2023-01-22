package LRU_cache

type Cache struct {
	capacity int
	size int
	list Dll
	hMap map[string]int	//golang mape ne podrzavaju byte arrayeve kao kljuceve, tako da moram da ga konvertujem u string, fuj
}

func (cache *Cache) IsFull() bool{
	return (*cache).list.IsFull()
}
func (cache *Cache) Init(capacity int){
	(*cache).capacity = capacity
	(*cache).size = 0
}
func (cache *Cache) Access(key []byte) []byte{
	el, ok := (*cache).hMap[string(key)]
	if ok{
		return (*cache).list.Get(el)
	}else{
		//Pristupi sstabeli i vrati element iz nje, dodaj je u mapu i listu
	}
	return []byte{}
}