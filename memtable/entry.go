package memtable

import (
	"time"
)

type MemTableEntry struct {
	key       []byte // Kljuc elementa
	value     []byte // Vrednost elementa
	timestamp int64  // Vreme kada je element dodat/promenjen u nanosekundama
	tombstone bool   // Da li je element obrisan
}

func createEntry(key []byte, value []byte) *MemTableEntry {
	return &MemTableEntry{key: key, value: value, timestamp: time.Now().UnixNano(), tombstone: false}
}
