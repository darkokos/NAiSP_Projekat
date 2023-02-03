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
}

func GetNewDB() *DB {
	config.ReadConfig()

	cache := LRU_cache.Cache{}
	cache.Init(int(config.Configuration.CacheSize))

	db := DB{cache: cache, memtable: *memtable.MakeMemTableFromConfig()}

	// Ponavlajmo sve operacije iz WAL-a
	db.CreateWalDirIfDoesNotExist()
	db.ReplayWal()

	return &db
}
