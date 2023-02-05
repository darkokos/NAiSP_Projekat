package memtable

import (
	"testing"
)

func TestRangeScanWithDeletions(t *testing.T) {
	memtable := MakeHashMapMemTable(10)

	memtable.Update("001", []byte{})
	memtable.Update("002", []byte{})
	memtable.Update("003", []byte{})
	memtable.Update("004", []byte{})

	memtable.Delete("002")

	if len(memtable.RangeScan("001", "004")) != 3 {
		t.Fatalf("Brisanje elemenata nije narusilo broj vracenih vrednosti")
	}
}
