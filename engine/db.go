package engine

import (
	"github.com/darkokos/NAiSP_Projekat/LRU_cache"
	"github.com/darkokos/NAiSP_Projekat/config"
	"github.com/darkokos/NAiSP_Projekat/memtable"
)

type DB struct {
	cache    LRU_cache.Cache
	memtable memtable.MemTable
	//lsm_tree *lsmtree.LogStructuredMergeTree
	wal_enabled bool
}

func GetNewDB() *DB {
	config.ReadConfig()

	cache := LRU_cache.Cache{}
	cache.Init(int(config.Configuration.CacheSize))

	db := DB{cache: cache, memtable: *memtable.MakeMemTableFromConfig(), wal_enabled: true}

	// Ponavlajmo sve operacije iz WAL-a
	db.CreateWalDirIfDoesNotExist()

	db.disableWALWriting()
	db.ReplayWal()
	db.enableWALWriting()

	return &db
}

func (engine *DB) disableWALWriting() {
	engine.wal_enabled = false
}

func (engine *DB) enableWALWriting() {
	engine.wal_enabled = true
}
