package LRU_cache

//Ovaj faj implementira dvostruko povezanu listu, za lru cache

type Dll struct {
	head     *DllElement
	tail     *DllElement
	capacity int
	size     int
}

// Jedan element
type DllElement struct {
	data []byte
	next *DllElement
	prev *DllElement
}

func (list *Dll) Init(size int) {
	(*list).capacity = size
	(*list).size = 0
}
func (list *Dll) IsFull() bool {
	return (*list).size == (*list).capacity
}
func (list *Dll) Pop(n int) DllElement {
	(*list).size--
	var cursor DllElement
	var x int
	if n == 0 {
		cursor = *(*list).head
		(*list).head = (*list.head).next
		return cursor
	} else if n == list.capacity {
		cursor = *(*list).tail
		(*list).tail = (*list.tail).prev
		return cursor
	}
	if n < list.capacity/2 {
		cursor = *(*list).head
		x = 1
	} else {
		cursor = *(*list).tail
		x = -1
	}
	for i := 0; i != n; i += x {
		cursor = *cursor.next
	}
	(*cursor.prev).next = cursor.next
	(*cursor.next).prev = cursor.prev

	cursor.next = nil //Common sense da bi se izbegle duplirane reference, verovatno nije neophodno
	cursor.prev = nil
	return cursor
}

// Jedan push prima samo podatke pa konstruise element, a drugi prima vec konstruisan element (go ne podrzava preklapanje metoda, grrrr)
func (list *Dll) Push(data []byte) {
	(*list).size++
	el := DllElement{data: data}
	if list.size == 1 {
		list.head = &el
		list.tail = &el
		return
	}
	(*list.head).prev = &el
	el.next = list.head
	(*list).head = &el
}

func (list *Dll) PushNode(el DllElement) {
	if (*list).IsFull() {
		(*list).DeleteLast()
	}
	(*list).size++
	(*list.head).prev = &el
	el.next = (*list).head
	(*list).head = &el
}
func (list *Dll) Get(n int) []byte {
	e := (*list).Pop(n)
	(*list).PushNode(e)
	return e.data
}
func (list *Dll) DeleteLast() {
	last := *(*list).tail
	(*list).tail = last.prev
	(*last.prev).next = nil
	last.prev = nil
	last.next = nil
	(*list).size--
}
func (list *Dll) GetLast() []byte {
	return (*list.tail).data
}
