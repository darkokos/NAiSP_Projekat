package config

import (
	"io/ioutil"

	"gopkg.in/yaml.v2"
)

var config Config = Config{
	WalSize:             10000000,
	MemtableSize:        4,
	MemtableStructure:   "skip_list",
	LSMTreeLevels:       4,
	MultipleFileSSTable: true,
	SummaryDensity:      4,
	CacheSize:           4,
	RateLimit:           3,
}

type Config struct {
	WalSize             uint64 `yaml:"wal_size"`               // Velicina WAL segmenta u bajtovima
	MemtableSize        uint64 `yaml:"memtable_size"`          // Max broj zapisa u MemTable (kad dodje do tog broja flush-uje se)
	MemtableStructure   string `yaml:"memtable_structure"`     // Koja struktura se koristi za memtable ("hashmap", "skip_list", "btree")
	LSMTreeLevels       uint64 `yaml:"lsm_tree_levels"`        // Maksimalan broj nivoa LSM stabla
	MultipleFileSSTable bool   `yaml:"sstable_multiple_files"` // Da li se SSTable pise kao vise fajlova
	SummaryDensity      uint64 `yaml:"summary_density"`        // Koliko zapisa pokriva jedan zapis u summary-u
	CacheSize           uint64 `yaml:"cache_size"`             // Broj elemenata u kesu
	RateLimit           uint64 `yaml:"rate_limit"`             // Rate limit za token bucket
}

func ReadConfig() {
	configData, err := ioutil.ReadFile("config.yml")
	if err != nil {
		return
	}
	yaml.Unmarshal(configData, &config)
	//fmt.Println(config)
}

// Procitacemo konfiguracioni fajl cim importujemo ovaj paket tj.
// na pocetku programa
func init() {
	ReadConfig()
}
