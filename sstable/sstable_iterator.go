package sstable

import (
	"io"
	"os"

	"github.com/darkokos/NAiSP_Projekat/utils"
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

func (iter *SSTableIterator) SeekToOffset(offset int64) {
	_, err := iter.sstFile.Seek(offset, io.SeekCurrent)
	if err != nil {
		iter.Valid = false
		iter.Ok = false
		iter.sstFile.Close()
	}

}

func (iter *SSTableIterator) SeekAndClose(key []byte) *SSTableEntry {

	//TODO: Mozda ne bi trebalo da radimo ova silna pretvaranja u stringove
	key_string := string(key)
	//defer iter.sstFile.Close()

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

func GetSSTableIterator(filename string) *SSTableIterator {
	//TODO: Osigurati da se zatvara fajl nakon return nil
	sstFile, err := os.Open(filename)
	if err != nil {
		return nil
	}

	magic_number := readMagicNumber(sstFile)

	size := utils.GetFileSize(filename)

	if size == -1 {
		return nil
	}

	if magic_number == SSTABLE_MULTI_FILE_MAGIC_NUMBER {
		end_of_sstable := size - SSTABLE_MAGIC_NUMBER_SIZE
		_, err := sstFile.Seek(0, io.SeekStart)
		if err != nil {
			return nil
		}

		iter := SSTableIterator{sstFile: sstFile, end_offset: end_of_sstable, Valid: true, Ok: true}
		return &iter

	} else if magic_number == SSTABALE_SINGLE_FILE_MAGIC_NUMBER {
		footer := ReadSSTFooter(sstFile)
		if footer == nil {
			return nil
		}

		// Moramo se vratiti na pocetak nakon citanja footer-a
		_, err = sstFile.Seek(0, io.SeekStart)
		if err != nil {
			return nil
		}

		iter := SSTableIterator{sstFile: sstFile, end_offset: footer.IndexOffset, Valid: true, Ok: true}
		return &iter

	} else {
		return nil // Sta god da smo procitali nije sstabela
	}

	return nil

}
