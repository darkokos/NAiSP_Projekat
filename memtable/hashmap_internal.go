package memtable

import "sort"

// Implementacija interfejsa MemTableInternal putem hes mape
type HashMapInternal struct {
	data map[string]*MemTableEntry
}

func makeHashMapInternal(capacity int) *HashMapInternal {
	return &HashMapInternal{data: make(map[string]*MemTableEntry, capacity)}
}

// Dobavlja MemTableEntry koji odgovara datom klucu i vraca true
// ili vraca nil i false ako element sa tim kljucem ne postoji
func (hashmap *HashMapInternal) Get(key string) (*MemTableEntry, bool) {
	v, ok := hashmap.data[key]
	return v, ok
}

// Dodaje ili menja element u strukturi
// Ako element postoji postavlja value polje odgovarajuceg MemTableEntry-a na value
// Ako element ne postoji, konstruise novi MemTableEntry i dodaje ga u strukturu
func (hashmap *HashMapInternal) Update(key string, value []byte) {
	v, ok := hashmap.data[key]
	if ok {
		v.Value = value
	} else {
		hashmap.data[key] = createEntry([]byte(key), value)
	}
}

// Dobavlja sve elemente iz strukture i vraca ih sortirane po kljucu u rastucem poretku
func (hashmap *HashMapInternal) GetSortedEntries() []*MemTableEntry {
	keys := make([]string, 0, len(hashmap.data))

	// Dobavljanje svih kljuceva iz mape
	for key := range hashmap.data {
		keys = append(keys, string(key))
	}

	sort.Strings(keys)

	entries := make([]*MemTableEntry, 0, len(hashmap.data))

	// Ubacujemo elemente iz mape u niz koji cemo vratiti
	for _, key := range keys {
		entries = append(entries, hashmap.data[key])
	}

	return entries
}

// Logicki brise element iz strukture time sto postavlja tombstone na true
// ako taj element postoji i vraca true.
// Ako ne postoji ne radi nista i vraca false.
func (hashmap *HashMapInternal) Delete(key string) bool {
	_, ok := hashmap.data[key]

	if !ok {
		return false
	} else {
		hashmap.data[key].Tombstone = true
		return true
	}
}

func (hashmap *HashMapInternal) Clear() {
	hashmap.data = make(map[string]*MemTableEntry, len(hashmap.data))
}

func (hashmap *HashMapInternal) Size() int {
	return len(hashmap.data)
}
