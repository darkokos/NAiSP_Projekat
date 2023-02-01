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
	Memtable     memtable.MemTable
	Level        int
	Currentlevel int
}

func NewLogStructuredMergeTree(capacity int) *LogStructuredMergeTree {
	return &LogStructuredMergeTree{
		Memtable:     *memtable.MakeHashMapMemTable(capacity),
		Level:        int(config.Configuration.LSMTreeLevels),
		Currentlevel: findlevel(),
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

	if value, ok := lsmt.Memtable.Get(string(key)); ok {
		return string(value), true
	} else {
		return lsmt.FindInSSTable(key)

	}
}

// Trazi kljuc u svim SS tabelama
func (lsmt *LogStructuredMergeTree) FindInSSTable(key []byte) (string, bool) {
	i := 1
	for i <= lsmt.Currentlevel {
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

// Funkcija uzima dva fajla, i pravi sortirani treci fajl
func merge(file1 string, file2 string, outputstring string) bool {
	iterator1 := sstable.GetSSTableIterator(file1)
	iterator2 := sstable.GetSSTableIterator(file2)
	writer := sstable.GetSSTFileWriter(config.Configuration.MultipleFileSSTable)
	writer.Open(outputstring)
	entry1 := iterator1.Next()
	entry2 := iterator2.Next()
	for {
		if iterator1.Valid && iterator2.Valid {
			if string(entry1.Key) < string(entry2.Key) {
				if !entry1.Tombstone {
					writer.Put(entry1)
				}
				entry1 = iterator1.Next()
				continue
			} else {
				if string(entry2.Key) < string(entry1.Key) {
					if !entry2.Tombstone {
						writer.Put(entry2)
					}
					entry2 = iterator2.Next()
					continue
				} else {
					if string(entry2.Key) == string(entry1.Key) {
						if entry1.Timestamp > entry2.Timestamp {
							if !entry1.Tombstone {
								writer.Put(entry1)
							} else {
								if !entry2.Tombstone {
									writer.Put(entry2)
								}
							}
							entry1 = iterator1.Next()
							entry2 = iterator2.Next()
							continue
						} else {
							if entry1.Timestamp < entry2.Timestamp {
								if !entry2.Tombstone {
									writer.Put(entry2)
								} else {
									if !entry1.Tombstone {
										writer.Put(entry1)
									}
								}
								entry1 = iterator1.Next()
								entry2 = iterator2.Next()
								continue
							}
						}
					}
				}

			}
		}
		if !iterator1.Valid && iterator2.Valid {
			if !entry2.Tombstone {
				writer.Put(entry2)
			}
			entry2 = iterator2.Next()
			continue
		}
		if iterator1.Valid && !iterator2.Valid {
			if !entry1.Tombstone {
				writer.Put(entry1)
			}
			entry1 = iterator1.Next()
			continue
		}
		if !iterator1.Valid && !iterator2.Valid {
			break
		}

	}
	writer.CloseFiles()
	return true
}
