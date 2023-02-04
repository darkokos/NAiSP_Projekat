package engine

import (
	"path/filepath"
	"strings"

	"github.com/darkokos/NAiSP_Projekat/config"
	"github.com/darkokos/NAiSP_Projekat/sstable"
)

// Funkcija radi range scan nad bazom.
// Vraca se niz vrednosti za kljuceve koji su u opsegu [begin, end]
// Ako je broj stranice veci od ukuponog broja stranica, vraca se prazan niz.
// Velicina strance se specificira parametrom page_size.
// Ako je opseg nije validan vraca se prazan niz.
// Paginacija pocinje stranicom 1. Ako se prosledi broj stranice 0, vraca se prazan niz.
func (engine *DB) RangeScan(begin string, end string, page_number uint, page_size uint) [][]byte {

	values := make([][]byte, 0)

	if begin > end || page_number < 1 || page_size == 0 {
		return values
	}

	// Prvo range-scan-ujemo memtable
	values = append(values, engine.memtable.RangeScan(begin, end)...)

	// Trazimo rezultate u sstabelama

	sstable_filenames, err := filepath.Glob("level-*-Data.db")
	if err != nil {
		sstable_filenames = make([]string, 0)
	}

	for _, sstable_filename := range sstable_filenames {
		index_filename := ""
		summary_filename := ""
		filter_filename := ""
		if config.Configuration.MultipleFileSSTable {
			index_filename = strings.Replace(sstable_filename, "-Data.db", "-Index.db", -1)
			summary_filename = strings.Replace(sstable_filename, "-Data.db", "-Summary.db", -1)
			filter_filename = strings.Replace(sstable_filename, "-Data.db", "-Filter.db", -1)
		}

		matchingEntries := sstable.RangeScanSSTable(begin, end, sstable_filename, index_filename, summary_filename, filter_filename)

		for _, entry := range matchingEntries {
			// Moramo proveriti da li je kljuc bio obrisan iz memtabele
			// Ako jeste onda je on trenutno obrisan i ne treba ga vratiti
			if !engine.memtable.IsDeleted(string(entry.Key)) {
				values = append(values, entry.Value)
			}

		}
	}

	begin_index_to_return := (page_number - 1) * page_size

	if len(values) < int(begin_index_to_return) {
		return [][]byte{}
	} else {
		return values[begin_index_to_return:min(int(begin_index_to_return)+int(page_size), len(values))]
	}

}

// Funkcija koja vraca minimum dva cela broja
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
