package engine

import wal "github.com/darkokos/NAiSP_Projekat/WAL"

func (engine *DB) Delete(key string) bool {
	//TODO: Delete operacija

	// Belezimo brisanje u WAL
	walEntry := wal.CreateWALEntry(true, []byte(key), []byte{})
	walEntry.Append()

	if r := recover(); r != nil {
		// Nije uspelo dodavanje u WAL
		return false
	}

	ok := engine.memtable.Delete(key)
	if ok {
		engine.cache.Edit([]byte(key), nil) // Moramo ukloniti element iz kesa - prevencija zastarelog kesa
		return true
	} else {
		return false
	}
}
