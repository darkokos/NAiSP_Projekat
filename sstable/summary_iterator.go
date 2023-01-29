package sstable

import (
	"encoding/binary"
	"io"
	"os"

	"github.com/darkokos/NAiSP_Projekat/utils"
)

type SummaryIterator struct {
	summaryFile *os.File
	end_offset  int64
	begin_key   string
	end_key     string
	Valid       bool
	Ok          bool
}

func getSummaryIteratorFromFile(filename string) *SummaryIterator {
	summary_file, err := os.Open(filename)

	if err != nil {
		return nil
	}

	size := utils.GetFileSize(filename)

	if size == -1 {
		return nil
	}

	size_bytes := make([]byte, 8)

	err = binary.Read(summary_file, binary.LittleEndian, size_bytes)
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

	iter := SummaryIterator{summaryFile: summary_file, end_offset: size, begin_key: string(first_key), end_key: string(last_key), Valid: true, Ok: true}

	return &iter
}

func getSummaryIteratorFromSSTableFile(filename string) *SummaryIterator {
	//TODO: Summary iterator iz sst fajla
	return nil
}

// Dobavlja sledeci summary zapis
// Ako se dodje do kraja summary-a ili se desi greska vraca nil i invalidara iterator i zatvara fajl
func (iter *SummaryIterator) Next() *SummaryEntry {
	cur_pos, _ := iter.summaryFile.Seek(0, io.SeekCurrent)

	if !iter.Valid {
		return nil
	}

	if cur_pos >= iter.end_offset {
		iter.Valid = false
		iter.Ok = true
		iter.summaryFile.Close()
		return nil
	}

	entry, ok := readSummaryEntry(iter.summaryFile)

	iter.Valid = (entry != nil)
	iter.Ok = ok

	if !iter.Valid {
		iter.summaryFile.Close()
	}

	return entry
}

// Nalazi zapis u summary-u u koji updada kljuc string.
// Iterator ostaje validan u ovom slucaju.
// Ili vraca nil, invalidira iterator i zatvara fajl
func (iter *SummaryIterator) Seek(key string) *SummaryEntry {

	if key < iter.begin_key || key > iter.end_key {
		iter.Valid = false
		iter.summaryFile.Close()
		return nil
	}

	for entry := iter.Next(); iter.Valid; entry = iter.Next() {
		if entry.FirstKey <= key && key <= entry.LastKey {
			return entry
		}
	}

	iter.summaryFile.Close()
	return nil
}

// Zatvara fajl i invalidira iterator
func (iter *SummaryIterator) Close() {
	iter.summaryFile.Close()
	iter.Valid = false
}
