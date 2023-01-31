package memtable

import (
	"time"
)

type MemTableEntry struct {
	Key       []byte // Kljuc elementa
	Value     []byte // Vrednost elementa
	Timestamp int64  // Vreme kada je element dodat/promenjen u nanosekundama
	Tombstone bool   // Da li je element obrisan
}

func createEntry(key []byte, value []byte) *MemTableEntry {
	return &MemTableEntry{Key: key, Value: value, Timestamp: time.Now().UnixNano(), Tombstone: false}
}
