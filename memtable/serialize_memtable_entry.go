package memtable

import (
	"bytes"
	"encoding/gob"
)

// Cita MemTableEntryIzBajtova
func MemTableEntryFromBytes(byte_arr []byte) *MemTableEntry {
	entry := &MemTableEntry{}
	decoder := gob.NewDecoder(bytes.NewBuffer(byte_arr[:]))
	err := decoder.Decode(entry)
	if err != nil {
		return nil
	}
	return entry
}

// Pretvara MemTableEntry u niz bajtova
func MemTableEntryToBytes(entry *MemTableEntry) []byte {
	buf := &bytes.Buffer{}
	encoder := gob.NewEncoder(buf)
	err := encoder.Encode(entry)
	if err != nil {
		panic(err) // TODO: Rukuj greskom u serijalizaciji memtable-entry-a
	}

	return buf.Bytes()
}
