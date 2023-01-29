package sstable

import (
	"encoding/binary"
	"io"
	"os"
)

type SSTableIterator struct {
	sstFile    *os.File
	Valid      bool  // Validnost iteratora, ako se doslo do kraja tablele i greske bice false
	Ok         bool  // True ako je nema greske, false ako je doslo do greske
	end_offset int64 // Pozicija u sstFile na kojem se zavrsava tabela
}

// Kako napraviti ovaj iterator
// Treba da znamo koju SST citamo (ime fajla)

// Treba nam da li je jedan ili vise fajlova
// Ovo znamo na osnovu formata
// Drugaciji magicni brojevi

// Kako dobijamo end_offset
// Na osnovu magicnog broja
// Ako je zaseban fajl - velicina - 8
// Ako nije zaseban fajl - imamo metaindex

func (iter *SSTableIterator) Next() *SSTableEntry {
	cur_pos, _ := iter.sstFile.Seek(0, io.SeekCurrent)

	if !iter.Valid {
		return nil
	}

	if cur_pos >= iter.end_offset {
		iter.Valid = false
		iter.Ok = true
		iter.sstFile.Close()
		return nil
	}

	entry, ok := ReadOneSSTEntry(iter.sstFile)

	iter.Valid = (entry != nil)
	iter.Ok = ok

	if !iter.Valid {
		iter.sstFile.Close()
	}

	return entry

}

func (iter *SSTableIterator) SeekAndClose(key []byte) *SSTableEntry {

	//TODO: Mozda ne bi trebalo da radimo ova silna pretvaranja u stringove
	key_string := string(key)
	defer iter.sstFile.Close()

	for entry := iter.Next(); iter.Valid; entry = iter.Next() {
		if string(entry.Key) == key_string {
			return entry
		}
	}

	return nil
}

// Zatvara fajl iteratora i invalidira ga
func (iter *SSTableIterator) Close() {
	iter.sstFile.Close()
	iter.Valid = false
}

func getSSTableIterator(filename string) *SSTableIterator {
	sstFile, err := os.Open(filename)
	if err != nil {
		return nil
	}

	stat, err := os.Stat(filename)
	if err != nil {
		return nil
	}

	size := stat.Size()

	magic_number_offset := size - SSTABLE_MAGIC_NUMBER_SIZE

	_, err = sstFile.Seek(magic_number_offset, io.SeekStart)
	if err != nil {
		return nil
	}

	magic_number_bytes := make([]byte, 8)
	err = binary.Read(sstFile, binary.LittleEndian, magic_number_bytes)
	if err != nil {
		return nil
	}
	magic_number := binary.LittleEndian.Uint64(magic_number_bytes)

	if magic_number == SSTABLE_MULTI_FILE_MAGIC_NUMBER {
		end_of_sstable := magic_number_offset
		_, err := sstFile.Seek(0, io.SeekStart)
		if err != nil {
			return nil
		}

		iter := SSTableIterator{sstFile: sstFile, end_offset: end_of_sstable, Valid: true, Ok: true}
		return &iter

	} else if magic_number == SSTABALE_SINGLE_FILE_MAGIC_NUMBER {
		//TODO: Konstrukcija iteratora za sstabelu koja je jedan fajl
	} else {
		return nil // Sta god da smo procitali nije sstabela
	}

	return nil

}
