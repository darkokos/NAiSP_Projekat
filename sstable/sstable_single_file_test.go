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

func TestReadWholeSSTableSingleFile(t *testing.T) {

	iter := GetSSTableIterator("test_table-Data.db")

	if iter == nil {
		t.Fatalf("Doslo je do greske u otvaranju sstabele")
	}

	number_of_entries := 0
	for entry := iter.Next(); iter.Valid; entry = iter.Next() {
		fmt.Println("Kljuc: ", string(entry.Key), " Vrednost: ", entry.Value)
		number_of_entries++
	}

	if number_of_entries != 5 {
		t.Fatalf("Nisu procitani svi redovi")
	}

	if !iter.Ok {
		t.Fatalf("Doslo je do greske u citanju sstable")
	}
}
