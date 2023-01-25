package memtable

import (
	"time"
)

type MemTableEntry struct {
	key       []byte
	value     []byte
	timestamp int64
	tombstone bool
}

func createEntry(key []byte, value []byte) *MemTableEntry {
	return &MemTableEntry{key: key, value: value, timestamp: time.Now().UnixMicro(), tombstone: false}
}
