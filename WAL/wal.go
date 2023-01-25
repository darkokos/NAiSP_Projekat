//TODO:
//Koristiti mmap za trenutni segment
//Odrediti kako ce se WAL koristiti, da bi znali sta da se radi sa procitanim podacima
//Primeniti low-water mark mehanizam
//Ispravke

package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"
)

/*
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   |    CRC (4B)   | Timestamp (8B) | Tombstone(1B) | Key Size (8B) | Value Size (8B) | Key | Value |
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   CRC = 32bit hash computed over the payload using CRC
   Key Size = Length of the Key data
   Tombstone = If this record was deleted and has a value
   Value Size = Length of the Value data
   Key = Key data
   Value = Value data
   Timestamp = Timestamp of the operation in seconds
*/

const (
	CRC_SIZE        = 4
	TIMESTAMP_SIZE  = 8
	TOMBSTONE_SIZE  = 1
	KEY_SIZE_SIZE   = 8
	VALUE_SIZE_SIZE = 8

	CRC_START        = 0
	TIMESTAMP_START  = CRC_START + CRC_SIZE
	TOMBSTONE_START  = TIMESTAMP_START + TIMESTAMP_SIZE
	KEY_SIZE_START   = TOMBSTONE_START + TOMBSTONE_SIZE
	VALUE_SIZE_START = KEY_SIZE_START + KEY_SIZE_SIZE
	KEY_START        = VALUE_SIZE_START + VALUE_SIZE_SIZE
)

type WALEntry struct { //Jedan zapis u WAL-u
	CRC       uint32
	Timestamp int64
	Tombstone bool
	KeySize   uint64
	ValueSize uint64
	Key       []byte
	Value     []byte
}

func CreateWALEntry(tombstone bool, key, value []byte) WALEntry { //Pravljenje novog zapisa WAL-a
	crc := crc32.NewIEEE()

	timestamp := time.Now().UnixNano()
	b := make([]byte, TIMESTAMP_SIZE)
	binary.BigEndian.PutUint64(b, uint64(timestamp))
	crc.Write(b)

	b = make([]byte, TOMBSTONE_SIZE)
	if tombstone {
		b[0] = 1
	}
	crc.Write(b)

	keySize := uint64(len(key))
	b = make([]byte, KEY_SIZE_SIZE)
	binary.BigEndian.PutUint64(b, keySize)
	crc.Write(b)

	valueSize := uint64(len(value))
	b = make([]byte, VALUE_SIZE_SIZE)
	binary.BigEndian.PutUint64(b, valueSize)
	crc.Write(b)

	crc.Write(key)

	crc.Write(value)

	return WALEntry{CRC: crc.Sum32(), Timestamp: timestamp, Tombstone: tombstone, KeySize: keySize, ValueSize: valueSize, Key: key, Value: value}
}

func (walEntry WALEntry) append() { //Dodavanje zapisa u aktuelni WAL fajl
	files, err := ioutil.ReadDir("wal/")
	if err != nil {
		panic(err)
	}

	filename := ""
	if len(files) == 0 {
		filename = "wal/wal_1.log"
	} else {
		filename = "wal/" + files[len(files)-1].Name()
	}

	file, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND, 0222)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	ret := make([]byte, 0)

	crc := walEntry.CRC
	b := make([]byte, CRC_SIZE)
	binary.BigEndian.PutUint32(b, crc)
	ret = append(ret, b...)

	timestamp := walEntry.Timestamp
	b = make([]byte, TIMESTAMP_SIZE)
	binary.BigEndian.PutUint64(b, uint64(timestamp))
	ret = append(ret, b...)

	b = make([]byte, TOMBSTONE_SIZE)
	if walEntry.Tombstone {
		b[0] = 1
	}
	ret = append(ret, b...)

	keySize := uint64(len(walEntry.Key))
	b = make([]byte, KEY_SIZE_SIZE)
	binary.BigEndian.PutUint64(b, keySize)
	ret = append(ret, b...)

	valueSize := uint64(len(walEntry.Value))
	b = make([]byte, VALUE_SIZE_SIZE)
	binary.BigEndian.PutUint64(b, valueSize)
	ret = append(ret, b...)

	ret = append(ret, walEntry.Key...)

	ret = append(ret, walEntry.Value...)

	fileInfo, err := os.Stat(filename)
	if err != nil {
		panic(err)
	}

	if fileInfo.Size()+int64(len(ret)) > 65 { //Pravljenje novog WAL fajla u slucaju da je trenutni popunjen
		offset, err := strconv.Atoi(strings.Split(filename[:len(filename)-4], "_")[1])
		if err != nil {
			panic(err)
		}

		file, err = os.OpenFile("wal/wal_"+strconv.Itoa(offset+1)+".log", os.O_CREATE|os.O_APPEND, 0222)
		if err != nil {
			panic(err)
		}
	}

	file.Write(ret)
}

func ReadWAL() { //Citanje aktuelnog WAL fajla
	files, err := ioutil.ReadDir("wal/")
	if err != nil {
		panic(err)
	}

	filename := ""
	if len(files) == 0 {
		panic("Nema WAL-a")
	} else {
		filename = "wal/" + files[len(files)-1].Name()
	}

	file, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	for {
		walEntry := new(WALEntry)

		b := make([]byte, CRC_SIZE)
		_, err = file.Read(b)
		if err != nil {
			break
		}

		walEntry.CRC = binary.BigEndian.Uint32(b)

		b = make([]byte, TIMESTAMP_SIZE)
		_, err = file.Read(b)
		if err != nil {
			panic(err)
		}

		walEntry.Timestamp = int64(binary.BigEndian.Uint64(b))

		b = make([]byte, TOMBSTONE_SIZE)
		_, err = file.Read(b)
		if err != nil {
			panic(err)
		}

		if b[0] == 1 {
			walEntry.Tombstone = true
		} else {
			walEntry.Tombstone = false
		}

		b = make([]byte, KEY_SIZE_SIZE)
		_, err = file.Read(b)
		if err != nil {
			panic(err)
		}

		walEntry.KeySize = binary.BigEndian.Uint64(b)

		b = make([]byte, VALUE_SIZE_SIZE)
		_, err = file.Read(b)
		if err != nil {
			panic(err)
		}

		walEntry.ValueSize = binary.BigEndian.Uint64(b)

		b = make([]byte, walEntry.KeySize)
		_, err = file.Read(b)
		if err != nil {
			panic(err)
		}

		walEntry.Key = b

		b = make([]byte, walEntry.ValueSize)
		_, err = file.Read(b)
		if err != nil {
			panic(err)
		}

		walEntry.Value = b

		fmt.Println(*walEntry) //Za sad se svaki zapis samo ispisuje u konzoli, jer jos ne znam sta raditi sa njima
	}
}
