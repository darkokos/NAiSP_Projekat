package memtable

type Entry struct {
	value     []byte
	tombstone bool
}

type MemTableEntry struct {
	key   string
	value []byte
}

func createEntry(value []byte) *Entry {
	return &Entry{value: value, tombstone: false}
}
