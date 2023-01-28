package sstable

import (
	"encoding/binary"
	"os"

	wal "github.com/darkokos/NAiSP_Projekat/WAL"
	"github.com/darkokos/NAiSP_Projekat/memtable"
)

// Pise deo summary-a koji sadrzi granice sstabele
func writeSummaryHeader(f *os.File, first *memtable.MemTableEntry, last *memtable.MemTableEntry) {
	begin_key_size_bytes := make([]byte, wal.KEY_SIZE_SIZE)
	end_key_size_bytes := make([]byte, wal.KEY_SIZE_SIZE)

	binary.LittleEndian.PutUint64(begin_key_size_bytes, uint64(len(first.Key)))
	binary.LittleEndian.PutUint64(end_key_size_bytes, uint64(len(last.Key)))

	err := binary.Write(f, binary.LittleEndian, begin_key_size_bytes)
	if err != nil {
		panic(err)
	}

	err = binary.Write(f, binary.LittleEndian, end_key_size_bytes)
	if err != nil {
		panic(err)
	}

	err = binary.Write(f, binary.LittleEndian, first.Key)
	if err != nil {
		panic(err)
	}

	err = binary.Write(f, binary.LittleEndian, last.Key)
	if err != nil {
		panic(err)
	}

}

//Ospezi u summary-u su intervali oblika [pocetak, kraj)
func writeSummaryEntry(f *os.File, first *memtable.MemTableEntry, last *memtable.MemTableEntry, offset int64) {
	writeSummaryHeader(f, first, last)

	offset_bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(offset_bytes, uint64(offset))

	binary.Write(f, binary.LittleEndian, offset_bytes)
}
