package sstable

import (
	"fmt"
	"os"
	"testing"

	"github.com/darkokos/NAiSP_Projekat/memtable"
)

func TestSSTable(t *testing.T) {
	fmt.Println("Kompajliralo se")

	sorted_entries := make([]*memtable.MemTableEntry, 0)

	sorted_entries = append(sorted_entries, memtable.CreateEntry([]byte("Darko"), []byte{'S', 'V', 50, 21}))
	sorted_entries = append(sorted_entries, memtable.CreateEntry([]byte("Gojko"), []byte{49, 21}))
	sorted_entries = append(sorted_entries, memtable.CreateEntry([]byte("Marko"), []byte("SV 38/2021")))
	sorted_entries = append(sorted_entries, memtable.CreateEntry([]byte("Momir"), []byte{39, 21, 1, 2, 3}))
	sorted_entries = append(sorted_entries, memtable.CreateEntry([]byte("Vuk"), []byte{52, 21}))

	WriteSSTableMultipleFiles("test_table", sorted_entries)
}

func TestReadWholeSSTable(t *testing.T) {

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

func TestReadSSTableByKeyMultipleFiles(t *testing.T) {

	entry := ReadOneSSTEntryWithKey([]byte("Gojko"), "test_table-Data.db", "test_table-Index.db", "test_table-Summary.db", "test_table-Filter.db")

	if entry == nil {
		t.Fatalf("Trebalo je da nadje entry")
	} else {
		if string(entry.Key) != "Gojko" {
			t.Fatalf("Kljuc se ne poklapa")
		}
	}

	entry = ReadOneSSTEntryWithKey([]byte("Vuk"), "test_table-Data.db", "test_table-Index.db", "test_table-Summary.db", "test_table-Filter.db")

	if entry == nil {
		t.Fatalf("Trebalo je da nadje entry")
	} else {
		if string(entry.Key) != "Vuk" {
			t.Fatalf("Kljuc se ne poklapa")
		}
	}

	entry = ReadOneSSTEntryWithKey([]byte("Darko"), "test_table-Data.db", "test_table-Index.db", "test_table-Summary.db", "test_table-Filter.db")

	if entry == nil {
		t.Fatalf("Trebalo je da nadje entry")
	} else {
		if string(entry.Key) != "Darko" {
			t.Fatalf("Kljuc se ne poklapa")
		}
	}

	entry = ReadOneSSTEntryWithKey([]byte("Momia"), "test_table-Data.db", "test_table-Index.db", "test_table-Summary.db", "test_table-Filter.db")

	if entry != nil {
		t.Fatalf("Nije trebalo da nadje ovo")
	}

}

func TestSSTableReadNonExistentFile(t *testing.T) {
	f, _ := os.Open("nepostojeci_fajl")

	entry, ok := ReadOneSSTEntry(f)

	if (entry != nil) || ok {
		t.Fatalf("Citanje iz postojeceg fajla nije trebalo da uspe")
	}
}

func TestSSTableCRCFail(t *testing.T) {
	f, err := os.OpenFile("test_table-Data.db", os.O_RDWR, 0222)

	if err != nil {
		t.Fatalf("Greska u otvaranju SSTabele")
	}

	f.Seek(0, 0)

	// Upisivanje pogresnog crc-a
	f.Write([]byte{0, 0, 0, 0})

	f.Close()

	f, err = os.Open("test_table-Data.db")

	if err != nil {
		t.Fatalf("Greska u otvaranju SSTabele")
	}

	_, ok := ReadOneSSTEntry(f)
	f.Close()

	if ok {
		t.Fatalf("Citanja zapisa sa pogresnim CRC-om ne bi trebalo da uspe")
	}

	iter := GetSSTableIterator("test_table-Data.db")

	iter.Next()

	if iter.Valid {
		t.Fatalf("Citanje zapisa sa pogresnim CRC-om bi trebalo da invalidira iterator")
	}
}
