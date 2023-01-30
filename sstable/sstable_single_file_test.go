package sstable

import (
	"fmt"
	"testing"

	"github.com/darkokos/NAiSP_Projekat/memtable"
)

func TestSSTableSingleFile(t *testing.T) {
	fmt.Println("Kompajliralo se")

	sorted_entries := make([]*memtable.MemTableEntry, 0)

	sorted_entries = append(sorted_entries, memtable.CreateEntry([]byte("Darko"), []byte{'S', 'V', 50, 21}))
	sorted_entries = append(sorted_entries, memtable.CreateEntry([]byte("Gojko"), []byte{49, 21}))
	sorted_entries = append(sorted_entries, memtable.CreateEntry([]byte("Marko"), []byte("SV 38/2021")))
	sorted_entries = append(sorted_entries, memtable.CreateEntry([]byte("Momir"), []byte{39, 21, 1, 2, 3}))
	sorted_entries = append(sorted_entries, memtable.CreateEntry([]byte("Vuk"), []byte{52, 21}))

	WriteSSTableOneFile("test_table", sorted_entries)
}
