package engine

import (
	wal "github.com/darkokos/NAiSP_Projekat/WAL"
)

func (engine *DB) Put(key string, value []byte) bool {
	key_bytes := []byte(key)

	entry := wal.CreateWALEntry(false, key_bytes, value)
	entry.Append()
	if r := recover(); r != nil {
		// Nije uspelo dodavanje u WAL
		return false
	} else {
		engine.cache.Edit(key_bytes, value) // Prevencija zastarelog kesa
		wal.DeleteSegments()
		return engine.memtable.Update(key, value)
	}

}
