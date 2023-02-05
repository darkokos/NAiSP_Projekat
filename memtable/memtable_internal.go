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
	// Elementi koji su logicki obrisani se gledaju kao da postoje. Dobice novi key-value i tombstone ce postati false
	Update(key string, value []byte)

	// Dobavlja sve elemente (i logicki obrisane) iz strukture i vraca ih sortirane po kljucu u rastucem poretku
	GetSortedEntries() []*MemTableEntry

	// Logicki brise element iz strukture time sto postavlja tombstone na true
	// ako taj element postoji i vraca true. Takodje se vrednost postavlja na
	// prazan niz.
	// Ako postoji element sa tim kljucem i tombstone-om postavljenim na true, ne radi nista i vraca false.
	// Ako ne postoji dodaje MemTable entry sa prosledjenim kljucem, praznim nizom
	// kao vrednoscu i tombstone-om postavljenim na true.
	Delete(key string) bool

	// Fizicki brise sve elemente iz strukture
	Clear()

	// Vraca broj elemenata u strukturi
	// U ovaj broj su ukljuceni i logicki obrisani elementi
	Size() int
}
