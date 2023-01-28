package sstable

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"time"

	wal "github.com/darkokos/NAiSP_Projekat/WAL"
	"github.com/darkokos/NAiSP_Projekat/memtable"
)

// Funkcija zapisuje niz MemTableEntry-a u SSTable sa imenom filename.
//
// Format SSTable-a:
//
//   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
//   |    CRC (4B)   | Timestamp (8B)  | Tombstone(1B) | Key Size (8B) | Value Size (8B) | Key | Value |
//   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
//   CRC = 32bitni hes nad ostatkom reda izracunat pomocu CRC-a
//   Key Size = Duzina kljuca u bajtovima
//   Tombstone = 1 ako je podatak obrisan 0 ako je aktuelan i ima vrednost
//   Value Size = Duzina vrednosti u bajtovima
//   Key = Kljuc
//   Value = Vrednost
//   Timestamp = Vreme kreiranja podataka izrazeno u nanosekundama

//TODO: Pravljenje dodatnih delova izdvojiti
func writeSSTable(filename string, sortedEntries []*memtable.MemTableEntry) {
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}

	//TODO:Ime indeks fajla
	indexFile, err := os.OpenFile("index.db", os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}

	//TODO: Ime summary fajla
	summaryFile, err := os.OpenFile("summary.db", os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}

	//TODO: Ime summary fajla
	filterFile, err := os.OpenFile("filter.db", os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}

	writeSummaryHeader(summaryFile, sortedEntries[0], sortedEntries[len(sortedEntries)-1])

	current_begin := sortedEntries[0]
	summary_density := 2 // TODO: Popunjenost summary-a treba da se konfigurise
	until_next_summary_entry := summary_density
	current_index_offset := int64(0)

	for _, entry := range sortedEntries {
		if until_next_summary_entry == summary_density {
			current_begin = entry
		}
		key := entry.Key
		//value := entry.Value
		offset, err := f.Seek(0, io.SeekCurrent)
		if err != nil {
			// handle error
			panic(err)
		}

		writeIndexEntry(indexFile, string(key), uint64(offset))
		//fmt.Println("Kljuc: ", key, "Vrednost: ", value)
		writeSSTableEntry(f, entry)

		until_next_summary_entry--
		if until_next_summary_entry == 0 {
			until_next_summary_entry = summary_density
			writeSummaryEntry(summaryFile, current_begin, entry, current_index_offset)
			current_index_offset, err = indexFile.Seek(0, io.SeekCurrent)
			if err != nil {
				// handle error
				panic(err)
			}
		}
	}

	if until_next_summary_entry != summary_density {
		writeSummaryEntry(summaryFile, current_begin, sortedEntries[len(sortedEntries)-1], current_index_offset)
	}

	writeFilter(filterFile, sortedEntries)
	writeMetadataSeparateFile(sortedEntries, "metadata.db")

	f.Close()
	indexFile.Close()
	summaryFile.Close()
	filterFile.Close()
}

// Funkcija pise red SSTable-a koji cuva prosledjeni
// podatak
func writeSSTableEntry(sstableFile *os.File, entry *memtable.MemTableEntry) {
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

// Format zapisa u indeksu je duzina kljuca (8B) - offset (8B) - kljuc(?B)
func writeIndexEntry(indexFile *os.File, key string, offset uint64) {

	binary.Write(indexFile, binary.LittleEndian, uint64(len(key)))
	binary.Write(indexFile, binary.LittleEndian, offset)
	binary.Write(indexFile, binary.LittleEndian, []byte(key))
}
