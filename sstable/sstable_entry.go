package sstable

import (
	wal "github.com/darkokos/NAiSP_Projekat/WAL"
	"github.com/darkokos/NAiSP_Projekat/memtable"
)

// Format SSTable i WAL-a su isti
type SSTableEntry wal.WALEntry

func CreateSSTableEntryFromMemTableEntry(memtableEntry *memtable.MemTableEntry) *SSTableEntry {

	entry := SSTableEntry{
		CRC:       CalculateCRC(memtableEntry.Timestamp, memtableEntry.Tombstone, uint64(len(memtableEntry.Key)), uint64(len(memtableEntry.Value)), memtableEntry.Key, memtableEntry.Value),
		Timestamp: memtableEntry.Timestamp,
		KeySize:   uint64(len(memtableEntry.Key)),
		ValueSize: uint64(len(memtableEntry.Value)),
		Key:       memtableEntry.Key,
		Value:     memtableEntry.Value,
	}

	return &entry
}
