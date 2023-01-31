package memtable

import (
	skiplist "github.com/darkokos/NAiSP_Projekat/SkipList"
)

const (
	MAX_SKIP_LIST_HEIGHT = 5
)

// Implementacija interfejsa MemTableInternal putem hes mape
type SkipListInternal struct {
	data *skiplist.SkipList
}

func MakeSkipListInternal() *SkipListInternal {
	return &SkipListInternal{data: skiplist.NewSkipList(MAX_SKIP_LIST_HEIGHT)}
}

// Dobavlja MemTableEntry koji odgovara datom klucu i vraca true
// ili vraca nil i false ako element sa tim kljucem ne postoji
func (skiplistInternal *SkipListInternal) Get(key string) (*MemTableEntry, bool) {
	v := skiplistInternal.data.Search(key)
	if v == nil {
		return nil, false
	} else {
		return memTableEntryFromBytes(v), true
	}
}

// Dodaje ili menja element u strukturi
// Ako element postoji postavlja value polje odgovarajuceg MemTableEntry-a na value
// Ako element ne postoji, konstruise novi MemTableEntry i dodaje ga u strukturu
func (skiplistInternal *SkipListInternal) Update(key string, value []byte) {
	newEntry := CreateEntry([]byte(key), value)

	updateSuccesful := skiplistInternal.data.Update(key, memTableEntryToBytes(newEntry))
	if !updateSuccesful {
		skiplistInternal.data.Insert(key, memTableEntryToBytes(newEntry))
	}
}

// Dobavlja sve elemente iz strukture i vraca ih sortirane po kljucu u rastucem poretku
func (skiplistInternal *SkipListInternal) GetSortedEntries() []*MemTableEntry {
	entries := make([]*MemTableEntry, 0, skiplistInternal.Size())

	// Ubacujemo elemente iz mape u niz koji cemo vratiti
	for _, value := range skiplistInternal.data.FirstLevelValues() {
		entries = append(entries, memTableEntryFromBytes(value))
	}

	return entries
}

// Logicki brise element iz strukture time sto postavlja tombstone na true
// ako taj element postoji i vraca true.
// Ako ne postoji ne radi nista i vraca false.
func (skiplistInternal *SkipListInternal) Delete(key string) bool {
	v := skiplistInternal.data.Search(key)

	if v != nil {
		entry := memTableEntryFromBytes(v)
		if entry != nil {
			if entry.Tombstone {
				return false
			}

			entry.Tombstone = true
			skiplistInternal.data.Update(key, memTableEntryToBytes(entry))
			return true
		} else {
			return false
		}
	} else {
		return false
	}
}

func (skiplistInternal *SkipListInternal) Clear() {
	skiplistInternal.data = skiplist.NewSkipList(MAX_SKIP_LIST_HEIGHT)
}

func (skiplistInternal *SkipListInternal) Size() int {
	return skiplistInternal.data.Size
}
