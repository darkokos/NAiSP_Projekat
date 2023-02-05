package lsmtree

import (
	"os"
	"testing"
)

/*
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
*/

func TestFindLevelNoSStables(t *testing.T) {
	if Findlevel() != 0 {
		t.Fatalf("Ne bi trebalo da postoji ni jedan nivo")
	}
}

func TestFindLevel(t *testing.T) {
	lvl1, _ := os.OpenFile("level-01-usertable-00000000000001-Data.db", os.O_CREATE|os.O_WRONLY, 0666)
	lvl1.Close()
	defer os.Remove(lvl1.Name())

	lvl1_1, _ := os.OpenFile("level-01-usertable-000000000002-Data.db", os.O_CREATE|os.O_WRONLY, 0666)
	lvl1_1.Close()
	defer os.Remove(lvl1_1.Name())

	lvl2_1, _ := os.OpenFile("level-02-usertable-00000000003-Data.db", os.O_CREATE|os.O_WRONLY, 0666)
	lvl2_1.Close()
	defer os.Remove(lvl2_1.Name())

	lvl3_1, _ := os.OpenFile("level-03-usertable-00000000004-Data.db", os.O_CREATE|os.O_WRONLY, 0666)
	lvl3_1.Close()
	defer os.Remove(lvl3_1.Name())

	lvl3_2, _ := os.OpenFile("level-03-usertable-00000000007-Data.db", os.O_CREATE|os.O_WRONLY, 0666)
	lvl3_2.Close()
	defer os.Remove(lvl3_2.Name())

	if Findlevel() != 3 {
		t.Fatalf("Pronadjen je pogresan broj nivoa %d", Findlevel())
	}
}

func TestFindLevelAntiCrash(t *testing.T) {
	lvl1, _ := os.OpenFile("level-Data.db", os.O_CREATE|os.O_WRONLY, 0666)
	lvl1.Close()
	defer os.Remove(lvl1.Name())

	lvl1_1, _ := os.OpenFile("level-.db", os.O_CREATE|os.O_WRONLY, 0666)
	lvl1_1.Close()
	defer os.Remove(lvl1_1.Name())

	if Findlevel() != 0 {
		t.Fatalf("Pronadjen je pogresan broj nivoa %d", Findlevel())
	}
}
