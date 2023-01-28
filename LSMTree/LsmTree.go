//TODO: Implementirati da radi sa nasim memtable i sstables

package lsmtree

import (
	"bytes"
	"encoding/binary"
)

// Ovo je samo prototip kako bi trebalo da radi
type LogStructuredMergeTree struct {
	memtable       map[string][]byte
	sstables       [][]byte
	mergeThreshold int
}

func NewLogStructuredMergeTree(mergeThreshold int) *LogStructuredMergeTree {
	return &LogStructuredMergeTree{
		memtable:       make(map[string][]byte),
		sstables:       [][]byte{},
		mergeThreshold: mergeThreshold,
	}
}

func (lsmt *LogStructuredMergeTree) Put(key []byte, value []byte) { //stavlja entry u memtable, a ako je pun pravi novu sstabelu
	lsmt.memtable[string(key)] = value
	if len(lsmt.memtable) >= lsmt.mergeThreshold {

		lsmt.merge()
	}
}

func (lsmt *LogStructuredMergeTree) Get(key []byte) (string, bool) { //trazi prvo u memtable, ako nije tamo prolazi kroz svaki sstable

	if value, ok := lsmt.memtable[string(key)]; ok {
		return string(value), true
	}
	for _, sstable := range lsmt.sstables {
		if len(sstable) < len(key)+4 {
			continue
		}
		if bytes.Compare(sstable[:len(key)], key) == 0 {
			valueSize := binary.LittleEndian.Uint32(sstable[len(key) : len(key)+4])
			valueStart := len(key) + 4
			valueEnd := valueStart + int(valueSize)
			if len(sstable) < valueEnd {
				continue
			}
			return string(sstable[valueStart:valueEnd]), true
		}
	}
	return "", false
}

func (lsmt *LogStructuredMergeTree) merge() { //pravljenje sstabele
	var newSSTable []byte
	for key, value := range lsmt.memtable {
		k := []byte(key)
		newSSTable = append(newSSTable, k...)
		var valueSize = make([]byte, 4)
		binary.LittleEndian.PutUint32(valueSize, uint32(len(value)))
		newSSTable = append(newSSTable, valueSize...)
		newSSTable = append(newSSTable, value...)
	}
	lsmt.sstables = append(lsmt.sstables, newSSTable)
	lsmt.memtable = make(map[string][]byte)
}
