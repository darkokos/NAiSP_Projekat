package lsmtree

import (
	"fmt"
	"strconv"

	"github.com/darkokos/NAiSP_Projekat/config"
	"github.com/darkokos/NAiSP_Projekat/memtable"
	"github.com/darkokos/NAiSP_Projekat/sstable"
)

// Pravi LSMTree sa memtabelom,maksimalnim nivoom i trenutnim najvecim nivoom
type LogStructuredMergeTree struct {
	memtable     memtable.MemTable
	level        int
	currentlevel int
}

func NewLogStructuredMergeTree(capacity int) *LogStructuredMergeTree {
	return &LogStructuredMergeTree{
		memtable:     *memtable.MakeHashMapMemTable(capacity),
		level:        int(config.Configuration.LSMTreeLevels),
		currentlevel: findlevel(),
	}

}

// Trazi trenutni najveci nivo
func findlevel() int {
	i := 1
	for {
		iter := sstable.GetSSTableIterator("level-" + fmt.Sprint(i) + "-usertable-000001--Data.db")
		if iter == nil {
			return i
		}
		i++
	}
}

// Trazi u memtable, ako ne nadje trazi u ss tabelama
func (lsmt *LogStructuredMergeTree) Get(key []byte) (string, bool) { //trazi prvo u memtable, ako nije tamo prolazi kroz svaki sstable

	if value, ok := lsmt.memtable.Get(string(key)); ok {
		return string(value), true
	} else {
		return lsmt.FindInSSTable(key)

	}
}

// Trazi kljuc u svim SS tabelama
func (lsmt *LogStructuredMergeTree) FindInSSTable(key []byte) (string, bool) {
	i := 1
	for i <= lsmt.currentlevel {
		levelstr := ""
		if i < 10 {
			levelstr = "0" + fmt.Sprint(i)
		} else {
			levelstr = fmt.Sprint(i)
		}
		if i == 1 {
			entrystr := "level-" + levelstr + "-usertable-000001"
			entry := sstable.ReadOneSSTEntryWithKey(key, entrystr+"--Data.db", entrystr+"--Index.db", entrystr+"--Summary.db", entrystr+"--Filter.db")
			if entry == nil {
			} else {
				return string(entry.Value), true
			}
		} else {
			for j := 1; j < 3; j++ {
				entrystr := "level-" + levelstr + "-usertable-00000" + strconv.Itoa(j)
				entry := sstable.ReadOneSSTEntryWithKey(key, entrystr+"--Data.db", entrystr+"--Index.db", entrystr+"--Summary.db", entrystr+"--Filter.db")
				if entry == nil {
				} else {
					return string(entry.Value), true
				}
			}
		}

	}
	fmt.Println("Ne nalazi se ni u jednoj sstabeli niti memtable")
	return "", false

}

func MergeMultipleTables(files []string, outputfile string) bool {
	iterators := []sstable.SSTableIterator{}
	entries := []sstable.SSTableEntry{}
	writer := sstable.GetSSTFileWriter(config.Configuration.MultipleFileSSTable)
	writer.Open(outputfile)
	for i := range files {
		iterator := sstable.GetSSTableIterator(files[i])
		if iterator == nil {
			fmt.Println("Ne radi")
			continue
		}
		iterators = append(iterators, *iterator)
		entry := iterator.Next()
		if entry == nil {
			fmt.Println("Ne radi")
			continue
		}
		entries = append(entries, *entry)
	}
	for {
		min := &sstable.SSTableEntry{}
		for i := range entries {
			if i == 0 {
				min = &entries[0]
			} else {
				if string(min.Key) > string(entries[i].Key) {
					min = &entries[i]
				} else {

					if string(min.Key) == string(entries[i].Key) {

						if min.Timestamp < entries[i].Timestamp {
							min = &entries[i]
						}
					}

				}
			}
		}
		writer.Put(min)
		tempkey := string(min.Key)
		for i := range entries {
			if tempkey == string(entries[i].Key) {
				entry := iterators[i].Next()
				if entry == nil {
					iterators = append(iterators[:i], iterators[i+1:]...)
					entries = append(entries[:i], entries[i+1:]...)
					i--
				} else {
					entries[i] = *entry
				}
			}
		}
		if len(iterators) == 0 {
			writer.Finish()
			break
		}

	}
	return true
}
