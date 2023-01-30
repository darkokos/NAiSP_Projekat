package sstable

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
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

	WriteSSTableOneFile("test_table_fused", sorted_entries)
}

func TestReadWholeSSTableSingleFile(t *testing.T) {

	iter := GetSSTableIterator("test_table_fused-Data.db")

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

func TestSSTableIteratorNonExistentFile(t *testing.T) {
	sstIter := GetSSTableIterator("nepostojeci_fajl")

	if sstIter != nil && sstIter.Valid {
		t.Fatalf("Ne bi trebalo da se dobije iterator od nepostojeceg fajla")
	}
}

func TestSSTableIteratorMagicNumberOnly(t *testing.T) {

	f, _ := ioutil.TempFile("", "malformed_sst")
	defer os.Remove(f.Name())
	binary.Write(f, binary.LittleEndian, SSTABALE_SINGLE_FILE_MAGIC_NUMBER)
	filename := f.Name()
	f.Close()

	sstIter := GetSSTableIterator(filename)

	if sstIter != nil && sstIter.Valid {
		t.Fatalf("Ne bi trebalo da se dobije iterator od loseg sst fajla")
	}

}

func TestSSTableIteratorMalfomedAndEmpty(t *testing.T) {

	f, _ := ioutil.TempFile("", "malformed_sst")
	defer os.Remove(f.Name())
	binary.Write(f, binary.LittleEndian, SSTABALE_SINGLE_FILE_MAGIC_NUMBER)
	binary.Write(f, binary.LittleEndian, []byte{0, 0, 0, 0, 0, 0, 0, 0})
	binary.Write(f, binary.LittleEndian, []byte{0, 0, 0, 0, 0, 0, 0, 0})
	binary.Write(f, binary.LittleEndian, []byte{0, 0, 0, 0, 0, 0, 0, 0})
	binary.Write(f, binary.LittleEndian, []byte{0, 0, 0, 0, 0, 0, 0, 0})
	filename := f.Name()
	f.Close()

	sstIter := GetSSTableIterator(filename)

	if sstIter != nil && sstIter.Valid {
		t.Fatalf("Ne bi trebalo da se dobije iterator od loseg sst fajla")
	}
}
