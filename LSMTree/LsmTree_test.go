package lsmtree

import (
	"fmt"
	"testing"

	"github.com/darkokos/NAiSP_Projekat/config"
	"github.com/darkokos/NAiSP_Projekat/memtable"
	"github.com/darkokos/NAiSP_Projekat/sstable"
)

func Test(t *testing.T) {

	fmt.Println("Kompajliralo se")
	writer := sstable.GetSSTFileWriter(config.Configuration.MultipleFileSSTable)
	writer.Open("test_table1")
	writer.Put(sstable.CreateSSTableEntryFromMemTableEntry(memtable.CreateEntry([]byte("Darko"), []byte{'S', 'V', 50, 21})))
	writer.Put(sstable.CreateSSTableEntryFromMemTableEntry(memtable.CreateEntry([]byte("Gojko"), []byte{49, 21})))
	writer.Put(sstable.CreateSSTableEntryFromMemTableEntry(memtable.CreateEntry([]byte("Vuk"), []byte{52, 21})))
	writer.Finish()
	writer2 := sstable.GetSSTFileWriter(config.Configuration.MultipleFileSSTable)
	writer2.Open("test_table2")
	writer2.Put(sstable.CreateSSTableEntryFromMemTableEntry(memtable.CreateEntry([]byte("Gojko"), []byte{'S', 'V'})))
	writer2.Put(sstable.CreateSSTableEntryFromMemTableEntry(memtable.CreateEntry([]byte("Momir"), []byte{100, 100})))
	writer2.Put(sstable.CreateSSTableEntryFromMemTableEntry(memtable.CreateEntry([]byte("Random"), []byte{123, 52})))
	writer2.Finish()

	files := []string{"test_table1-Data.db", "test_table2-Data.db"}
	MergeMultipleTables(files, "test_table3")
	iter := sstable.GetSSTableIterator("test_table3-Data.db")
	for entry := iter.Next(); iter.Valid; entry = iter.Next() {
		fmt.Println("Kljuc: ", string(entry.Key), " Vrednost: ", entry.Value)
	}

}
