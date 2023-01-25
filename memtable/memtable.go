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
	memTable.data.Clear()
}

func (memTable *MemTable) Get(key string) ([]byte, bool) {
	//Vrati vrednost i uspesnost pretrage
	v, ok := memTable.data.Get(key)

	if ok {
		return v.Value, ok
	} else {
		return nil, ok
	}

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

// Funkcija proverava da li je element sa datim kljucem bio obrisan tj. da li
// postoji MemTableEntry sa tim kljucem i postavljenim tombstone-om na true
func (memTable *MemTable) IsDeleted(key string) bool {

	//FIXME: Moguce je da ne citamo dva puta iz strukture za svako citanje
	v, ok := memTable.data.Get(key)

	if !ok {
		return false
	} else {
		return v.Tombstone
	}
}

func (memTable *MemTable) Flush() {

	memTableEntries := memTable.data.GetSortedEntries()

	for _, entry := range memTableEntries {
		fmt.Println("Kljuc: ", string(entry.Key), "Vrednost: ", entry.Value, "Timestamp:", entry.Timestamp, "Obrisan: ", entry.Tombstone)
	}

	//TODO: Formiranje SSTable-a
	// Za sada se ispisuje sadrzaj na ekran
	// writeSSTable(fmt.Sprintf("usertable-%d-TABLE.db", memTable.generation), memTableEntries)

	memTable.generation = memTable.generation + 1
	memTable.remakeStructure()

	//Sort i ispisi na ekran
}
