package sstable

import (
	"encoding/binary"
	"os"
)

type IndexEntry struct {
	KeySize uint64
	Offset  int64
	Key     string
}

func readIndexEntry(indexFile *os.File) (*IndexEntry, bool) {
	key_size_bytes := make([]byte, 8)
	offset_bytes := make([]byte, 8)

	err := binary.Read(indexFile, binary.LittleEndian, key_size_bytes)
	if err != nil {
		return nil, false
	}

	err = binary.Read(indexFile, binary.LittleEndian, offset_bytes)
	if err != nil {
		return nil, false
	}

	key_size := binary.LittleEndian.Uint64(key_size_bytes)
	offset := int64(binary.LittleEndian.Uint64(offset_bytes))

	key_bytes := make([]byte, key_size)
	err = binary.Read(indexFile, binary.LittleEndian, key_bytes)
	if err != nil {
		return nil, false
	}

	indexEntry := IndexEntry{Key: string(key_bytes), KeySize: key_size, Offset: offset}

	return &indexEntry, true
}
