package memtable

import (
	"fmt"
)

type HashMapMemTable struct {
	data       *HashMapInternal
	capacity   int
	generation int
}

func makeHashMapMemTable(capacity int) *HashMapMemTable {
	memTable := HashMapMemTable{data: makeHashMapInternal(capacity), capacity: capacity, generation: 0}
	return &memTable
}

func (memTable *HashMapMemTable) remakeStructure() {
	fmt.Println("Made struct")
	memTable.data.Clear()
}

func (memTable *HashMapMemTable) Get(key string) ([]byte, bool) {
	//Vrati vrednost i uspesnost pretrage
	v, ok := memTable.data.Get(key)

	return v.value, ok

}

func (memTable *HashMapMemTable) Update(key string, value []byte) bool {
	//Promeni vrednost
	//Vrati uspesnost
	memTable.data.Update(key, value)

	if memTable.data.Size() == memTable.capacity {
		memTable.Flush()
	}

	return true
}

func (memTable *HashMapMemTable) Delete(key string) bool {
	//Logicko brisanje
	//Vrati uspesnost
	return memTable.data.Delete(key)
}

func (memTable *HashMapMemTable) Flush() {

	memTableEntries := memTable.data.GetSortedEntries()

	for _, entry := range memTableEntries {
		fmt.Println("Kljuc: ", string(entry.key), "Vrednost: ", entry.value, "Timestamp:", entry.timestamp, "Obrisan: ", entry.tombstone)
	}

	// writeSSTable(fmt.Sprintf("usertable-%d-TABLE.db", memTable.generation), memTableEntries)

	memTable.generation = memTable.generation + 1
	memTable.remakeStructure()

	//Sort i ispisi na ekran
}
