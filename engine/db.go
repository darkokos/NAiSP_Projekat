package engine

import (
	"github.com/darkokos/NAiSP_Projekat/LRU_cache"
	lsmtree "github.com/darkokos/NAiSP_Projekat/LSMTree"
)

type DB struct {
	cache    LRU_cache.Cache
	lsm_tree lsmtree.LogStructuredMergeTree
}
