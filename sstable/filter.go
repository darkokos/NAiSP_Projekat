package sstable

import (
	"encoding/binary"
	"os"

	bloomfilter "github.com/darkokos/NAiSP_Projekat/bloom-filter"
	"github.com/darkokos/NAiSP_Projekat/memtable"
)

const (
	FALSE_POSITIVE_RATE = 0.01
)

func writeFilter(f *os.File, entries []*memtable.MemTableEntry) {

	// TODO: Konfigurasti false-positive rate
	filter := bloomfilter.CreateBloomFilterBasedOnParams(len(entries), FALSE_POSITIVE_RATE)
	filter_bytes := filter.Serialize()

	serialized_length := uint64(len(filter_bytes))

	binary.Write(f, binary.LittleEndian, serialized_length)
	binary.Write(f, binary.LittleEndian, filter_bytes)
}
