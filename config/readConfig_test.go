package config

import "testing"

func TestReadConfig(t *testing.T) {

	ReadConfig()

	if Configuration.WalSize != 20000000 {
		t.Fatalf("Pogresno je procitana velicina wal segmenta")
	}

	if Configuration.MemtableSize != 10 {
		t.Fatalf("Pogresno je procitana velicina memtable")
	}

	if Configuration.MemtableStructure != "btree" {
		t.Fatalf("Pogresno je procitana koja struktura se koristi za memtable")
	}

	if Configuration.LSMTreeLevels != 4 {
		t.Fatalf("Pogresno je procitan broj nivoa LSM stabla")
	}

	if Configuration.SummaryDensity != 5 {
		t.Fatalf("Nije dobro procitana popunjenost summary-a")
	}

	if Configuration.CacheSize != 4 {
		t.Fatalf("Nije dobro procitana popunjenost cache-a")
	}

	if Configuration.RateLimit != 4 {
		t.Fatalf("Nije dobro procitan rate limit")
	}
}
