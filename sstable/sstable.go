package sstable

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/darkokos/NAiSP_Projekat/memtable"
)

func writeSSTable(filename string, sortedEntries []memtable.MemTableEntry) {
	//Format
	//Key_Size Val_Size Key Val
	//8B	   8B       ?   ?
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}

	//TODO:Ime indeks fajla
	indexFile, err := os.OpenFile("index.db", os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}

	for _, entry := range sortedEntries {
		key := entry.Key
		value := entry.Value
		offset, err := f.Seek(0, io.SeekCurrent)
		if err != nil {
			// handle error
		}
		writeIndexEntry(indexFile, string(key), uint64(offset))
		fmt.Println("Kljuc: ", key, "Vrednost: ", value)

		err = binary.Write(f, binary.LittleEndian, uint64(len(key)))
		if err != nil {
			panic(err.Error())
		}

		err = binary.Write(f, binary.LittleEndian, uint64(len(value)))
		if err != nil {
			panic(err.Error())
		}

		err = binary.Write(f, binary.LittleEndian, []byte(key))
		if err != nil {
			panic(err.Error())
		}

		err = binary.Write(f, binary.LittleEndian, value)
		if err != nil {
			panic(err.Error())
		}

		err = binary.Write(f, binary.LittleEndian, value)
		if err != nil {
			panic(err.Error())
		}

	}

	f.Close()
}

func writeIndexEntry(indexFile *os.File, key string, offset uint64) {

	binary.Write(indexFile, binary.LittleEndian, uint64(len(key)))
	binary.Write(indexFile, binary.LittleEndian, offset)
	binary.Write(indexFile, binary.LittleEndian, []byte(key))
}
