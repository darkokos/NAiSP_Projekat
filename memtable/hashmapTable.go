package memtable

import (
	"fmt"
	"sort"
)

type HashMapMemTable struct {
	data       map[string]*Entry
	capacity   int
	generation int
}

func makeHashMapMemTable(capacity int) *HashMapMemTable {
	memTable := HashMapMemTable{data: nil, capacity: capacity, generation: 0}
	memTable.remakeStructure()
	return &memTable
}

func (memTable *HashMapMemTable) remakeStructure() {
	fmt.Println("Made struct")
	memTable.data = make(map[string]*Entry)
}

func (memTable *HashMapMemTable) Get(key string) ([]byte, bool) {
	//Vrati vrednost i uspesnost pretrage
	v, ok := memTable.data[key]
	return v.value, ok

}

func (memTable *HashMapMemTable) Update(key string, value []byte) bool {
	//Promeni vrednost
	//Vrati uspesnost
	memTable.data[key] = createEntry(value)

	/*fmt.Println("UPDATE")
	fmt.Println(memTable.capacity)
	fmt.Println(len(memTable.data))
	fmt.Println("END")*/

	if len(memTable.data) >= memTable.capacity {
		memTable.Flush()
	}

	return true
}

func (memTable *HashMapMemTable) Delete(key string) bool {
	//Logicko brisanje
	//Vrati uspesnost
	v, ok := memTable.data[key]

	if !ok {
		return false
	} else {
		v.tombstone = true
		return true
	}

	return true
}

func (memTable *HashMapMemTable) Flush() {

	keys := make([]string, 0, len(memTable.data))
	fmt.Println(memTable.data)
	for key := range memTable.data {
		if !memTable.data[key].tombstone {
			keys = append(keys, string(key))
		}
	}

	sort.Strings(keys)

	memTableEntries := make([]MemTableEntry, len(keys))
	for idx, key := range keys {
		memTableEntries[idx] = MemTableEntry{key: key, value: memTable.data[key].value}
		fmt.Println("Kljuc: ", key, " Vrednost: ", memTableEntries[idx].value)
	}

	// writeSSTable(fmt.Sprintf("usertable-%d-TABLE.db", memTable.generation), memTableEntries)

	memTable.generation = memTable.generation + 1
	memTable.remakeStructure()

	//Sort i ispisi na ekran
}
