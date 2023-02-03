package memtable

import (
	"fmt"
	"time"

	"github.com/darkokos/NAiSP_Projekat/config"
	"github.com/darkokos/NAiSP_Projekat/sstable"
)

type MemTable struct {
	data       MemTableInternal
	capacity   int
	generation int
}

func MakeHashMapMemTable(capacity int) *MemTable {
	memTable := MemTable{data: MakeHashMapInternal(capacity), capacity: capacity, generation: 0}
	return &memTable
}

func MakeSkipListMemTable(capacity int) *MemTable {
	memTable := MemTable{data: MakeSkipListInternal(), capacity: capacity, generation: 0}
	return &memTable
}

func MakeBTreeMemTable(capacity int) *MemTable {
	memtable := MemTable{data: MakeBTreeInternal(), capacity: capacity, generation: 0}
	return &memtable
}

func MakeMemTableFromConfig() *MemTable {
	if config.Configuration.MemtableStructure == "hashmap" {
		return MakeHashMapMemTable(int(config.Configuration.MemtableSize))
	} else if config.Configuration.MemtableStructure == "skip_list" {
		return MakeSkipListMemTable(int(config.Configuration.MemtableSize))
	} else if config.Configuration.MemtableStructure == "b_tree" {
		return MakeBTreeMemTable(int(config.Configuration.MemtableSize))
	} else {
		fmt.Println("Nepoznata struktura za memtable, stavljam default")
		return MakeSkipListMemTable(int(config.DefaultConfiguration.MemtableSize))
	}
}

func (memTable *MemTable) remakeStructure() {
	memTable.data.Clear()
}

func (memTable *MemTable) Get(key string) ([]byte, bool) {
	//Vrati vrednost i uspesnost pretrage
	v, ok := memTable.data.Get(key)

	if ok {
		if !v.Tombstone {
			return v.Value, true
		} else {
			return nil, false
		}
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
	ret_val := memTable.data.Delete(key)

	//Brisanje moze izavati flush
	if memTable.data.Size() == memTable.capacity {
		memTable.Flush()
	}

	return ret_val
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

	fmt.Println("Flush")

	sstWriter := sstable.GetSSTFileWriter(config.Configuration.MultipleFileSSTable)

	current_time := time.Now().UnixNano() // Kao broj generacije cemo dodeliti vreme kad je sstabela kreirana
	sstWriter.Open("level-01-usertable-" + fmt.Sprintf("%020d", current_time))

	for _, entry := range memTableEntries {
		fmt.Println("Kljuc: ", string(entry.Key), "Vrednost: ", entry.Value, "Timestamp:", entry.Timestamp, "Obrisan: ", entry.Tombstone)

		// Mora ovaj copy-paste jer cemo izazvati cirkularni import
		sst_entry := sstable.CreateSSTableEntry(entry.Key, entry.Value, entry.Timestamp, entry.Tombstone)

		sstWriter.Put(sst_entry)
	}

	//TODO: Formiranje SSTable-a
	// Za sada se ispisuje sadrzaj na ekran
	// writeSSTable(fmt.Sprintf("usertable-%d-TABLE.db", memTable.generation), memTableEntries)

	sstWriter.Finish()

	memTable.generation = memTable.generation + 1
	memTable.remakeStructure()

	//Sort i ispisi na ekran
}
