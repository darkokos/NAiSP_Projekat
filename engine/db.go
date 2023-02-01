package engine

import (
	"github.com/darkokos/NAiSP_Projekat/LRU_cache"
	lsmtree "github.com/darkokos/NAiSP_Projekat/LSMTree"
	"github.com/darkokos/NAiSP_Projekat/config"
)

type DB struct {
	cache    LRU_cache.Cache
	lsm_tree *lsmtree.LogStructuredMergeTree
}

func GetNewDB() *DB {
	config.ReadConfig()

	cache := LRU_cache.Cache{}
	cache.Init(int(config.Configuration.CacheSize))

	db := DB{cache: cache, lsm_tree: lsmtree.NewLogStructuredMergeTree(int(config.Configuration.MemtableSize))}

	return &db
}
