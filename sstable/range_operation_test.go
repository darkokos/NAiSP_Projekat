package sstable

import (
	"testing"

	"github.com/darkokos/NAiSP_Projekat/config"
)

func TestRangeScan(t *testing.T) {
	config.DefaultConfiguration.SummaryDensity = 3 // Smanjujemo summary density da bi se adekvatno testirale provere summary-a
	config.ReadConfig()

	sorted_entries := make([]*SSTableEntry, 0)
	sorted_entries = append(sorted_entries, CreateFreshSSTableEntry([]byte("2020-04-06"), []byte{}))
	sorted_entries = append(sorted_entries, CreateFreshSSTableEntry([]byte("2020-04-07"), []byte{}))
	sorted_entries = append(sorted_entries, CreateFreshSSTableEntry([]byte("2020-04-08"), []byte{}))
	sorted_entries = append(sorted_entries, CreateFreshSSTableEntry([]byte("2022-01-01"), []byte{}))
	sorted_entries = append(sorted_entries, CreateFreshSSTableEntry([]byte("2022-01-02"), []byte{}))
	sorted_entries = append(sorted_entries, CreateFreshSSTableEntry([]byte("2022-05-05"), []byte{}))
	sorted_entries = append(sorted_entries, CreateFreshSSTableEntry([]byte("2023-01-17"), []byte{}))
	sorted_entries = append(sorted_entries, CreateFreshSSTableEntry([]byte("2023-03-03"), []byte{}))

	sstWriter := GetSSTFileWriter(false)
	sstWriter.Open("test_table_date_search")

	for _, sstEntry := range sorted_entries {
		sstWriter.Put(sstEntry)
	}

	sstWriter.Finish()

	if len(RangeScanSSTable("2020-01-01", "2024-01-01", "test_table_date_search-Data.db", "", "", "")) != 8 {
		t.Fatalf("Nije ucitan ispravan broj zapisa")
	}

	if len(RangeScanSSTable("1", "2", "test_table_date_search-Data.db", "", "", "")) != 0 {
		t.Fatalf("Nije ucitan ispravan broj zapisa")
	}

	if len(RangeScanSSTable("2", "3", "test_table_date_search-Data.db", "", "", "")) != 8 {
		t.Fatalf("Nije ucitan ispravan broj zapisa")
	}

	if len(RangeScanSSTable("2", "3", "test_table_date_search-Data.db", "", "", "")) != 8 {
		t.Fatalf("Nije ucitan ispravan broj zapisa")
	}

	if len(RangeScanSSTable("2022-01-01", "2022-02-01", "test_table_date_search-Data.db", "", "", "")) != 2 {
		t.Fatalf("Nije ucitan ispravan broj zapisa")
	}

	if len(RangeScanSSTable("2022-01-01", "2022-01-02", "test_table_date_search-Data.db", "", "", "")) != 2 {
		t.Fatalf("Nije ucitan ispravan broj zapisa")
	}

	if len(RangeScanSSTable("2022-01-01", "2022-01-01", "test_table_date_search-Data.db", "", "", "")) != 1 {
		t.Fatalf("Nije ucitan ispravan broj zapisa")
	}

	if len(RangeScanSSTable("2020-01-01", "2020-12-31", "test_table_date_search-Data.db", "", "", "")) != 3 {
		t.Fatalf("Nije ucitan ispravan broj zapisa")
	}

	if len(RangeScanSSTable("2023-01-01", "2023-12-31", "test_table_date_search-Data.db", "", "", "")) != 2 {
		t.Fatalf("Nije ucitan ispravan broj zapisa")
	}
}
