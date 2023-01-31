package sstable

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"time"

	wal "github.com/darkokos/NAiSP_Projekat/WAL"
	"github.com/darkokos/NAiSP_Projekat/memtable"
)

const (
	SSTABLE_MAGIC_NUMBER_SIZE         = 8                          // Velicina magicnog broja u bajtovima
	SSTABLE_MULTI_FILE_MAGIC_NUMBER   = uint64(0x473700DD14E7F08B) // Magicni broj za SSTabelu u rezimu gde je jedna SSTabele sacinjena iz vise fajlova
	SSTABALE_SINGLE_FILE_MAGIC_NUMBER = uint64(0xE14695378B12D2F8) // Magicni broj za SSTabelu u rezimu gde je su svi elementi SSTabele u jednom fajlu
)

// Funkcija zapisuje niz MemTableEntry-a u SSTable sa imenom filename.
//
// Format zapisa SSTable-a:
//
//	+---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
//	|    CRC (4B)   | Timestamp (8B)  | Tombstone(1B) | Key Size (8B) | Value Size (8B) | Key | Value |
//	+---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
//	CRC = 32bitni hes nad ostatkom reda izracunat pomocu CRC-a
//	Key Size = Duzina kljuca u bajtovima
//	Tombstone = 1 ako je podatak obrisan 0 ako je aktuelan i ima vrednost
//	Value Size = Duzina vrednosti u bajtovima
//	Key = Kljuc
//	Value = Vrednost
//	Timestamp = Vreme kreiranja podataka izrazeno u nanosekundama
//
// Format sstabele ako su elementi u zasebnim fajlovima:
// [data block 1]
// [data block 2]
// ...
// [data block n]
// [index block]
// [summary block]
// [filter block]
// [metadata block]
// [footer]
// [magic number]
// data block - Redovi/Zapisi SSTabele koji su formata opisanog iznad
// index block - Blok koji predstavlja index strukturu
// summary block - Blok koji predstavlja summary strukturu
// filter block - Blok u kojem zapisan bloom filter za kljuceve iz data blokova
// metadata block - Blok u kome je zapisano merkle stablo za sve vrednosti iz data blokova
// footer - Blok u kome se nalazi pokazivaci ka index, summary, filter i metadata blokovima (videti sst_footer.go)
// magic number - Magicni broj koji kaze da je SSTabela zapisana kao jedan fajl (8B)

// TODO: Sledece dve funkcije bi trebalo da budu jedna koja ce citati iz konfiguracije kako da pise

// Funkcija pise MemTable zapise u SSTabelu koja se nalazi u vise fajlova.
// filename_prefix predstavlja string koji ce se nalaziti pre -Data.db, -Index.db, ...
func WriteSSTableMultipleFiles(filename_prefix string, sortedEntries []*memtable.MemTableEntry) {

	sstWriter := GetSSTFileWriter(true)
	sstWriter.Open(filename_prefix)

	for _, entry := range sortedEntries {
		sstEntry := CreateSSTableEntryFromMemTableEntry(entry)
		sstWriter.Put(sstEntry)
	}

	sstWriter.Finish()
}

// Funkcija pise MemTable zapise u SSTabelu koja se nalazi u jendom fajlu.
// filename_prefix predstavlja string koji ce se nalaziti pre -Data.db.
// SSTabele koje su jedan fajl imaju samo Data fajl.
func WriteSSTableOneFile(filename_prefix string, sortedEntries []*memtable.MemTableEntry) {

	sstWriter := GetSSTFileWriter(false)
	sstWriter.Open(filename_prefix)

	for _, entry := range sortedEntries {
		sstEntry := CreateSSTableEntryFromMemTableEntry(entry)
		sstWriter.Put(sstEntry)
	}

	sstWriter.Finish()
}

// Ova funkcija se ne koristi.
// Funkcija pise red SSTable-a koji cuva prosledjeni memtable zapis
func writeSSTableEntryFromMemtable(sstableFile *os.File, entry *memtable.MemTableEntry) {
	crc := crc32.NewIEEE()

	timestamp := time.Now().UnixNano()
	timestamp_bytes := make([]byte, wal.TIMESTAMP_SIZE)
	binary.LittleEndian.PutUint64(timestamp_bytes, uint64(timestamp))
	crc.Write(timestamp_bytes)

	tombstone_byte := make([]byte, wal.TOMBSTONE_SIZE)
	if entry.Tombstone {
		tombstone_byte[0] = 1
	}
	crc.Write(tombstone_byte)

	keySize := uint64(len(entry.Key))
	key_size_bytes := make([]byte, wal.KEY_SIZE_SIZE)
	binary.LittleEndian.PutUint64(key_size_bytes, keySize)
	crc.Write(key_size_bytes)

	valueSize := uint64(len(entry.Value))
	value_size_bytes := make([]byte, wal.VALUE_SIZE_SIZE)
	binary.LittleEndian.PutUint64(value_size_bytes, valueSize)
	crc.Write(value_size_bytes)

	crc.Write(entry.Key)

	crc.Write(entry.Value)

	err := binary.Write(sstableFile, binary.LittleEndian, crc.Sum32())
	if err != nil {
		panic(err)
	}

	err = binary.Write(sstableFile, binary.LittleEndian, timestamp_bytes)
	if err != nil {
		panic(err)
	}

	err = binary.Write(sstableFile, binary.LittleEndian, tombstone_byte)
	if err != nil {
		panic(err)
	}

	err = binary.Write(sstableFile, binary.LittleEndian, key_size_bytes)
	if err != nil {
		panic(err)
	}

	err = binary.Write(sstableFile, binary.LittleEndian, value_size_bytes)
	if err != nil {
		panic(err)
	}

	err = binary.Write(sstableFile, binary.LittleEndian, entry.Key)
	if err != nil {
		panic(err)
	}

	err = binary.Write(sstableFile, binary.LittleEndian, entry.Value)
	if err != nil {
		panic(err)
	}
}

// Funkcija pise red SSTable-a koji cuva prosledjeni
// podatak
func writeSSTableEntry(sstableFile *os.File, entry *SSTableEntry) {
	timestamp := entry.Timestamp
	timestamp_bytes := make([]byte, wal.TIMESTAMP_SIZE)
	binary.LittleEndian.PutUint64(timestamp_bytes, uint64(timestamp))

	tombstone_byte := make([]byte, wal.TOMBSTONE_SIZE)
	if entry.Tombstone {
		tombstone_byte[0] = 1
	}

	keySize := entry.KeySize
	key_size_bytes := make([]byte, wal.KEY_SIZE_SIZE)
	binary.LittleEndian.PutUint64(key_size_bytes, keySize)

	valueSize := entry.ValueSize
	value_size_bytes := make([]byte, wal.VALUE_SIZE_SIZE)
	binary.LittleEndian.PutUint64(value_size_bytes, valueSize)

	err := binary.Write(sstableFile, binary.LittleEndian, entry.CRC)
	if err != nil {
		panic(err)
	}

	err = binary.Write(sstableFile, binary.LittleEndian, timestamp_bytes)
	if err != nil {
		panic(err)
	}

	err = binary.Write(sstableFile, binary.LittleEndian, tombstone_byte)
	if err != nil {
		panic(err)
	}

	err = binary.Write(sstableFile, binary.LittleEndian, key_size_bytes)
	if err != nil {
		panic(err)
	}

	err = binary.Write(sstableFile, binary.LittleEndian, value_size_bytes)
	if err != nil {
		panic(err)
	}

	err = binary.Write(sstableFile, binary.LittleEndian, entry.Key)
	if err != nil {
		panic(err)
	}

	err = binary.Write(sstableFile, binary.LittleEndian, entry.Value)
	if err != nil {
		panic(err)
	}
}
