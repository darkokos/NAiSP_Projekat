package sstable

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

	bloomfilter "github.com/darkokos/NAiSP_Projekat/bloom-filter"
	"github.com/darkokos/NAiSP_Projekat/merkleTree"
)

type SSTFileWriter struct {
	sstFile             *os.File
	summaryFile         *os.File
	indexFile           *os.File
	filterFile          *os.File
	metadataFile        *os.File
	tocFile             *os.File
	is_multiple_files   bool // TODO: Mozda cak ni ovo ne treba da bude tu kad budemo imali citanje konfiguracije
	records_written     int
	next_summary_key    []byte
	next_summary_offset int64
	last_key_written    []byte
	valuesWritten       [][]byte // Mora zbog merkle stabla
	Ok                  bool
}

// Funkcija konstruise jedan SSTFileWriter
func GetSSTFileWriter(mulitple_files bool) SSTFileWriter {
	//TODO: Citati multiple_files iz podesavanja
	return SSTFileWriter{sstFile: nil, summaryFile: nil, indexFile: nil, metadataFile: nil, tocFile: nil, is_multiple_files: mulitple_files, next_summary_key: []byte{}, next_summary_offset: 0, records_written: 0, last_key_written: []byte{}, Ok: true, valuesWritten: make([][]byte, 0)}
}

// Funkcija otvara fajl(ove) za upis SSTabele
//
// Ako je writer.multiple_files postavljen na true, funkcija otvara 5 fajlova,
// a u suprotnom otvara se samo sstFile, a ostali atributi tipa os.File ce
// ostati nil.
// Ako je doslo do greske, atribut Ok ce biti postavljen na false i bilo koji
// otvoreni fajlovi ce biti zatvoreni.
//
// Interpretacija parametra base name:
// Ako je is_multiple_files true: String koji ce stojati pre -Data.db/-Index.db/... u imenima fajlova
// Ako je is_multiple_files false: Celo ime SST fajla
func (writer *SSTFileWriter) Open(base_name string) {
	if writer.is_multiple_files {
		// Pisanje sstabele kao vise fajlova
		file_open_fail := false
		sstFile, err := os.OpenFile(base_name+"-Data.db", os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			file_open_fail = true
		}

		indexFile, err := os.OpenFile(base_name+"-Index.db", os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			file_open_fail = true
		}

		summaryFile, err := os.OpenFile(base_name+"-Summary.db", os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			file_open_fail = true
		}

		filterFile, err := os.OpenFile(base_name+"-Filter.db", os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			file_open_fail = true
		}

		metadataFile, err := os.OpenFile(base_name+"-Metadata.txt", os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			file_open_fail = true
		}

		tocFile, err := os.OpenFile(base_name+"-TOC.txt", os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			file_open_fail = true
		}

		if file_open_fail {
			writer.Ok = false
			sstFile.Close()
			indexFile.Close()
			summaryFile.Close()
			filterFile.Close()
			metadataFile.Close()
			tocFile.Close()
			return
		} else {
			writer.sstFile = sstFile
			writer.indexFile = indexFile
			writer.summaryFile = summaryFile
			writer.filterFile = filterFile
			writer.metadataFile = metadataFile
			writer.tocFile = tocFile
		}

	} else {
		// Pisanje sstabele kao jedan fajl
	}
}

func (writer *SSTFileWriter) Put(entry *SSTableEntry) {
	summary_density := 3
	if writer.is_multiple_files {
		if writer.records_written%summary_density == 0 {
			writer.next_summary_key = entry.Key
		}

		key := entry.Key
		//value := entry.Value
		offset, err := writer.sstFile.Seek(0, io.SeekCurrent)
		if err != nil {
			// handle error
			panic(err)
		}

		writeIndexEntry(writer.indexFile, string(key), uint64(offset))
		//fmt.Println("Kljuc: ", key, "Vrednost: ", value)
		writeSSTableEntry(writer.sstFile, entry)

		writer.valuesWritten = append(writer.valuesWritten, entry.Value)
		writer.records_written++
		if writer.records_written%summary_density == 0 {
			writeSummaryEntry(writer.summaryFile, writer.next_summary_key, entry.Key, writer.next_summary_offset)
			writer.next_summary_offset, err = writer.indexFile.Seek(0, io.SeekCurrent)
			if err != nil {
				// handle error
				panic(err)
			}
		}
	} else {

	}
}

func (writer *SSTFileWriter) CloseFiles() {
	writer.sstFile.Close()
	if writer.is_multiple_files {
		writer.indexFile.Close()
		writer.summaryFile.Close()
		writer.filterFile.Close()
		writer.metadataFile.Close()
		writer.tocFile.Close()
	}
}

func (writer *SSTFileWriter) Finish() {
	summary_density := 3 //TODO: I ovde zameniti summary_density

	sstIter := GetSSTableIterator(writer.sstFile.Name())

	filter := bloomfilter.CreateBloomFilterBasedOnParams(writer.records_written, FALSE_POSITIVE_RATE)

	for entry := sstIter.Next(); sstIter.Valid; entry = sstIter.Next() {
		filter.Add(entry.Key)
	}

	serialized_filter := filter.Serialize()
	serialized_length := uint64(len(serialized_filter))

	metadata := merkleTree.CreateMerkleTree(writer.valuesWritten)
	metadataBytes := merkleTree.SerializeTree(metadata)

	if writer.is_multiple_files {
		if writer.records_written%summary_density != 0 {
			writeSummaryEntry(writer.summaryFile, writer.next_summary_key, writer.last_key_written, writer.next_summary_offset)
		}

		// I sta cemo sad
		// Imamo records written
		// Imamo sstable
		// I iterator :)
		// U oba slucaja smo pisali samo u sstable

		// Zapisivanje bloom filtera
		err := binary.Write(writer.filterFile, binary.LittleEndian, serialized_length)
		if err != nil {
			writer.Ok = false
			writer.CloseFiles()
			return
		}

		binary.Write(writer.filterFile, binary.LittleEndian, serialized_filter)
		if err != nil {
			writer.Ok = false
			writer.CloseFiles()
			return
		}

		//Zapisivanje Merkle Stala
		err = binary.Write(writer.metadataFile, binary.LittleEndian, metadataBytes)
		if err != nil {
			fmt.Println("Greska u zapsivanju merkle stabla")
			writer.Ok = false
			writer.CloseFiles()
			return
		}

		toc_contents := writer.sstFile.Name() + "\n" + writer.indexFile.Name() +
			"\n" + writer.summaryFile.Name() + "\n" + writer.filterFile.Name() + "\n" +
			writer.metadataFile.Name() + writer.tocFile.Name()

		_, err = writer.tocFile.Write([]byte(toc_contents))
		if err != nil {
			writer.Ok = false
			writer.CloseFiles()
			return
		}

		err = binary.Write(writer.sstFile, binary.LittleEndian, SSTABLE_MULTI_FILE_MAGIC_NUMBER)
		if err != nil {
			panic(err)
		}

	}
	writer.CloseFiles()
}
