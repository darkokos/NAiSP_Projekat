package sstable

//TODO: Citanje iz spojenog fajla
//TODO: Trazenje po indeksu

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

	wal "github.com/darkokos/NAiSP_Projekat/WAL"
)

// Funkcija pokusava da procita sledeci zapis u SSTabeli na koju pokazuje fajl
// deskriptor sstableFile.
// Funkcija vraca par koji se sastoji od zapisa u SSTabeli i bool-a koji govori
// da li je citanje bilo uspesno.
// Funkcija vraca sledeci zapis i true ako uspesno procita zapis.
// Funkcija vraca nil i true ako nema vise zapisa koji treba da se procitaju.
// Funckcija vraca nil i false ako je doslo do greske u citanju fajla.
func ReadOneSSTEntry(sstableFile *os.File) (entry *SSTableEntry, ok bool) {
	// Mozda da se vracaju dva bool-a, hasNext i err
	//s := SSTableEntry

	crc_bytes := make([]byte, wal.CRC_SIZE)
	timestamp_bytes := make([]byte, wal.TIMESTAMP_SIZE)
	tombstone_byte := make([]byte, wal.TOMBSTONE_SIZE)
	key_size_bytes := make([]byte, wal.KEY_SIZE_SIZE)
	value_size_bytes := make([]byte, wal.VALUE_SIZE_SIZE)

	//TODO: Copy paste-ovanje binary.Read-a nije bas lepo
	err := binary.Read(sstableFile, binary.LittleEndian, crc_bytes)
	if err != nil {
		if err == io.EOF {
			return nil, true
		}
		return nil, false
	}

	err = binary.Read(sstableFile, binary.LittleEndian, timestamp_bytes)
	if err != nil {
		return nil, false
	}

	err = binary.Read(sstableFile, binary.LittleEndian, tombstone_byte)
	if err != nil {
		return nil, false
	}

	err = binary.Read(sstableFile, binary.LittleEndian, key_size_bytes)
	if err != nil {
		return nil, false
	}

	err = binary.Read(sstableFile, binary.LittleEndian, value_size_bytes)
	if err != nil {
		return nil, false
	}

	crc := binary.LittleEndian.Uint32(crc_bytes)
	timestamp := binary.LittleEndian.Uint64(timestamp_bytes)

	tombstone := false
	if tombstone_byte[0] == 1 {
		tombstone = true
	}

	key_size := binary.LittleEndian.Uint64(key_size_bytes)
	value_size := binary.LittleEndian.Uint64(value_size_bytes)

	// TODO: key_size i value_size mogu biti poprilicno veliki, treba se zastiti od toga
	key_bytes := make([]byte, key_size)
	value_bytes := make([]byte, value_size)

	err = binary.Read(sstableFile, binary.LittleEndian, key_bytes)
	if err != nil {
		return nil, false
	}

	err = binary.Read(sstableFile, binary.LittleEndian, value_bytes)
	if err != nil {
		return nil, false
	}

	entry = &SSTableEntry{
		CRC:       crc,
		Timestamp: int64(timestamp),
		Tombstone: tombstone,
		KeySize:   key_size,
		ValueSize: value_size,
		Key:       key_bytes,
		Value:     value_bytes,
	}

	if CheckSSTEntryCRC(entry) {
		return entry, true
	} else {
		os.Stderr.WriteString("CRC provera nije uspela")
		return nil, false
	}
}

//TODO: Wrapper oko ovoga za imena fajlova

func readOneSSTEntryWithKeyMultipleFiles(key []byte, filterFile *os.File, summaryFile *os.File, indexFile *os.File, sstFile *os.File) (entry *SSTableEntry) {
	key_string := string(key)
	filter := readFilter(filterFile)

	if !filter.Find(key) {
		return nil
	}

	summaryEntry := findSummaryEntry(summaryFile, key)
	if summaryEntry == nil {
		return nil
	}

	//stat, _ := os.Stat(indexFile.Name())
	//fmt.Println("Velicina indeksa: ", stat.Size())
	_, err := indexFile.Seek(summaryEntry.Offset, io.SeekCurrent)
	if err != nil {
		fmt.Println("Greska pri citanju indeksa: ", err)
		return nil
	}

	currentIndexEntry, _ := readIndexEntry(indexFile)

	for currentIndexEntry != nil && currentIndexEntry.Key <= summaryEntry.LastKey {
		if currentIndexEntry.Key == key_string {
			_, err := sstFile.Seek(currentIndexEntry.Offset, io.SeekCurrent)
			if err != nil {
				fmt.Println("Greska: ", err)
				fmt.Println(int(currentIndexEntry.Offset))
				return nil
			}

			sstEntry, _ := ReadOneSSTEntry(sstFile)
			return sstEntry
		}
		currentIndexEntry, _ = readIndexEntry(indexFile)
	}

	return nil

	//Proveri opseg summary-a
	//Citaj summary dok ne naidjes
	//Procitaj indeks
	//Procitaj entry
}

func ReadOneSSTEntryWithKey(key []byte, sstFileName string, indexFilename string, summaryFilename string, filterFilename string) *SSTableEntry {

	sstFile, err := os.Open(sstFileName)

	if err != nil {
		return nil
	}
	defer sstFile.Close()

	// Slucaj citanja iz vise fajlova
	if indexFilename != "" {
		filterFile, err := os.Open(filterFilename)

		if err != nil {
			return nil
		}
		defer filterFile.Close()

		summaryFile, err := os.Open(summaryFilename)

		if err != nil {
			return nil
		}
		defer summaryFile.Close()

		indexFile, err := os.Open(indexFilename)
		if err != nil {
			return nil
		}

		return readOneSSTEntryWithKeyMultipleFiles(key, filterFile, summaryFile, indexFile, sstFile)
	} else {
		//TODO: Citanje iz sstable-a koji je jedan fajl
	}

	return nil
}
