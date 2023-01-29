package sstable

import (
	"encoding/binary"
	"io"
	"os"

	wal "github.com/darkokos/NAiSP_Projekat/WAL"
)

type SummaryEntry struct {
	FirstKey string
	LastKey  string
	Offset   int64
}

// Pise deo summary-a koji sadrzi granice sstabele
func writeSummaryHeader(f *os.File, first []byte, last []byte) {
	begin_key_size_bytes := make([]byte, wal.KEY_SIZE_SIZE)
	end_key_size_bytes := make([]byte, wal.KEY_SIZE_SIZE)

	binary.LittleEndian.PutUint64(begin_key_size_bytes, uint64(len(first)))
	binary.LittleEndian.PutUint64(end_key_size_bytes, uint64(len(last)))

	err := binary.Write(f, binary.LittleEndian, begin_key_size_bytes)
	if err != nil {
		panic(err)
	}

	err = binary.Write(f, binary.LittleEndian, end_key_size_bytes)
	if err != nil {
		panic(err)
	}

	err = binary.Write(f, binary.LittleEndian, first)
	if err != nil {
		panic(err)
	}

	err = binary.Write(f, binary.LittleEndian, last)
	if err != nil {
		panic(err)
	}

}

//Ospezi u summary-u su intervali oblika [pocetak, kraj)
func writeSummaryEntry(f *os.File, first []byte, last []byte, offset int64) {
	writeSummaryHeader(f, first, last)

	offset_bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(offset_bytes, uint64(offset))

	binary.Write(f, binary.LittleEndian, offset_bytes)
}

func readSummaryEntry(summary_file *os.File) (*SummaryEntry, bool) {

	size_bytes := make([]byte, 8)

	err := binary.Read(summary_file, binary.LittleEndian, size_bytes)
	if err != nil {
		if err == io.EOF {
			return nil, true
		}
		return nil, false
	}

	first_key_size := binary.LittleEndian.Uint64(size_bytes)

	err = binary.Read(summary_file, binary.LittleEndian, size_bytes)
	if err != nil {
		return nil, false
	}

	last_key_size := binary.LittleEndian.Uint64(size_bytes)

	//TODO: Ozbediti se od lose ucitanih (ovo se moze uraditi proverom u odnosu na velicinu fajla)
	first_key := make([]byte, first_key_size)
	last_key := make([]byte, last_key_size)
	binary.Read(summary_file, binary.LittleEndian, first_key)
	binary.Read(summary_file, binary.LittleEndian, last_key)

	binary.Read(summary_file, binary.LittleEndian, size_bytes)
	offset := binary.LittleEndian.Uint64(size_bytes)

	summaryEntry := SummaryEntry{FirstKey: string(first_key), LastKey: string(last_key), Offset: int64(offset)}

	return &summaryEntry, true
}

// Vraca summary zapis u ciji opseg upada key ili nil ako takvog zapisa nema ili dodje do greske
func findSummaryEntry(summary_file *os.File, key []byte) *SummaryEntry {

	key_string := string(key)

	size_bytes := make([]byte, 8)

	err := binary.Read(summary_file, binary.LittleEndian, size_bytes)
	if err != nil {
		return nil
	}

	first_key_size := binary.LittleEndian.Uint64(size_bytes)

	err = binary.Read(summary_file, binary.LittleEndian, size_bytes)
	if err != nil {
		return nil
	}

	last_key_size := binary.LittleEndian.Uint64(size_bytes)

	//TODO: Ozbediti se od lose ucitanih (ovo se moze uraditi proverom u odnosu na velicinu fajla)
	first_key := make([]byte, first_key_size)
	last_key := make([]byte, last_key_size)
	binary.Read(summary_file, binary.LittleEndian, first_key)
	binary.Read(summary_file, binary.LittleEndian, last_key)

	if key_string < string(first_key) || key_string > string(last_key) {
		return nil
	}

	currentSummaryEntry, _ := readSummaryEntry(summary_file)

	for currentSummaryEntry != nil {
		if currentSummaryEntry.FirstKey <= key_string && key_string <= currentSummaryEntry.LastKey {
			return currentSummaryEntry
		}

		if currentSummaryEntry.LastKey == string(last_key) {
			break
		}

		currentSummaryEntry, _ = readSummaryEntry(summary_file)
	}

	return nil
}
