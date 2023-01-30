package config

import "testing"

func TestReadConfig(t *testing.T) {

	ReadConfig()

	if config.WalSize != 20000000 {
		t.Fatalf("Pogresno je procitana velicina wal segmenta")
	}

	if config.MemtableSize != 10 {
		t.Fatalf("Pogresno je procitana velicina memtable")
	}

	if config.MemtableStructure != "btree" {
		t.Fatalf("Pogresno je procitana koja struktura se koristi za memtable")
	}

	if config.LSMTreeLevels != 4 {
		t.Fatalf("Pogresno je procitan broj nivoa LSM stabla")
	}

	if config.SummaryDensity != 5 {
		t.Fatalf("Nije dobro procitana popunjenost summary-a")
	}

	if config.CacheSize != 4 {
		t.Fatalf("Nije dobro procitana popunjenost cache-a")
	}

	if config.RateLimit != 4 {
		t.Fatalf("Nije dobro procitan rate limit")
	}
}
