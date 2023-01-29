package sstable

import (
	"encoding/binary"
	"io"
	"os"
)

type IndexEntry struct {
	KeySize uint64
	Offset  int64
	Key     string
}

// Funkcija cita sledeci zapis indeksa
// Vraca procitani zapis i true ako ima zapisa i uspesno je procitan
// Vraca nil i true ako nema vise zapisa
// Varac nil i false ako je doslo do greske
func readIndexEntry(indexFile *os.File) (*IndexEntry, bool) {
	key_size_bytes := make([]byte, 8)
	offset_bytes := make([]byte, 8)

	err := binary.Read(indexFile, binary.LittleEndian, key_size_bytes)
	if err != nil {
		if err == io.EOF {
			return nil, true
		}
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

// Format zapisa u indeksu je duzina kljuca (8B) - offset (8B) - kljuc(?B)
func writeIndexEntry(indexFile *os.File, key string, offset uint64) {

	binary.Write(indexFile, binary.LittleEndian, uint64(len(key)))
	binary.Write(indexFile, binary.LittleEndian, offset)
	binary.Write(indexFile, binary.LittleEndian, []byte(key))
}
