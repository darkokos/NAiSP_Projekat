package sstable

import (
	"encoding/binary"
	"hash/crc32"

	wal "github.com/darkokos/NAiSP_Projekat/WAL"
)

// Funkcija proverava da li CRC u prosledjenom SSTableEntry-u odgovara ostatku njegovog sadrzaja.
// Funkcija vraca true ako je CRC checksum validan, a false ako nije.
func CheckSSTEntryCRC(entry *SSTableEntry) bool {

	// TODO: Kod za racunanje CRC-a SST/WAL zapisa izolovati u svoju funkciju
	crc_calculated := crc32.NewIEEE()

	b := make([]byte, wal.TIMESTAMP_SIZE)
	binary.LittleEndian.PutUint64(b, uint64(entry.Timestamp))
	crc_calculated.Write(b)

	b = make([]byte, wal.TOMBSTONE_SIZE)
	if entry.Tombstone {
		b[0] = 1
	}
	crc_calculated.Write(b)

	keySize := entry.KeySize
	b = make([]byte, wal.KEY_SIZE_SIZE)
	binary.LittleEndian.PutUint64(b, keySize)
	crc_calculated.Write(b)

	valueSize := entry.ValueSize
	b = make([]byte, wal.VALUE_SIZE_SIZE)
	binary.LittleEndian.PutUint64(b, valueSize)
	crc_calculated.Write(b)

	crc_calculated.Write(entry.Key)

	crc_calculated.Write(entry.Value)

	return crc_calculated.Sum32() == entry.CRC
}
