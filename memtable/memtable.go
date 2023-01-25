package memtable

import (
	"fmt"
)

type MemTable struct {
	data       *HashMapInternal
	capacity   int
	generation int
}

func makeHashMapMemTable(capacity int) *MemTable {
	memTable := MemTable{data: makeHashMapInternal(capacity), capacity: capacity, generation: 0}
	return &memTable
}

func (memTable *MemTable) remakeStructure() {
	fmt.Println("Made struct")
	memTable.data.Clear()
}

func (memTable *MemTable) Get(key string) ([]byte, bool) {
	//Vrati vrednost i uspesnost pretrage
	v, ok := memTable.data.Get(key)

	return v.value, ok

}

func (memTable *MemTable) Update(key string, value []byte) bool {
	//Promeni vrednost
	//Vrati uspesnost
	memTable.data.Update(key, value)

	if memTable.data.Size() == memTable.capacity {
		memTable.Flush()
	}

	return true
}

func (memTable *MemTable) Delete(key string) bool {
	//Logicko brisanje
	//Vrati uspesnost
	return memTable.data.Delete(key)
}

func (memTable *MemTable) Flush() {

	memTableEntries := memTable.data.GetSortedEntries()

	for _, entry := range memTableEntries {
		fmt.Println("Kljuc: ", string(entry.key), "Vrednost: ", entry.value, "Timestamp:", entry.timestamp, "Obrisan: ", entry.tombstone)
	}

	// writeSSTable(fmt.Sprintf("usertable-%d-TABLE.db", memTable.generation), memTableEntries)

	memTable.generation = memTable.generation + 1
	memTable.remakeStructure()

	//Sort i ispisi na ekran
}
