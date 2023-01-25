package memtable

// Interfejs predstavlja strukuturu koja se interno koristi u memtable-u
// za cuvanje podataka
type MemTableInternal interface {
	// Dobavlja MemTableEntry koji odgovara datom klucu i vraca true
	// ili vraca nil i false ako element sa tim kljucem ne postoji
	Get(key string) (*MemTableEntry, bool)

	// Dodaje ili menja element u strukturi
	// Ako element postoji postavlja value polje odgovarajuceg MemTableEntry-a na value
	// Ako element ne postoji, konstruise novi MemTableEntry i dodaje ga u strukturu
	Update(key string, value []byte)

	// Dobavlja sve elemente iz strukture i vraca ih sortirane po kljucu u rastucem poretku
	GetSortedEntries() []*MemTableEntry

	// Logicki brise element iz strukture time sto postavlja tombstone na true
	// ako taj element postoji i vraca true.
	// Ako ne postoji ne radi nista i vraca false.
	Delete(key string) bool

	// Fizicki brise sve elemente iz strukture
	Clear()

	// Vraca broj elemenata u strukturi
	Size() int
}
