//TODO: -Implementirati da uzima maksimalan nivo iz configa
//      -Pravljenje SSTabela(mada ne znam da li ovo treba u memtable da se implementira ili ovde)

package lsmtree

import (
	"fmt"
	"strconv"

	memtable "github.com/darkokos/NAiSP_Projekat/memtable"
	sstable "github.com/darkokos/NAiSP_Projekat/sstable"
)

// Pravi LSMTree sa memtabelom,ss tabelama,maksimalnim nivoom i trenutnim najvecim nivoom
type LogStructuredMergeTree struct {
	memtable     memtable.MemTable
	sstables     []string
	level        int
	currentlevel int
}

func NewLogStructuredMergeTree(capacity int, LSMlevel int) *LogStructuredMergeTree {
	return &LogStructuredMergeTree{
		memtable:     *memtable.MakeHashMapMemTable(capacity),
		sstables:     make([]string, 0),
		level:        LSMlevel,
		currentlevel: findlevel(),
	}
}

// Trazi trenutni najveci nivo
func findlevel() int {
	i := 1
	for {
		iter := sstable.GetSSTableIterator("level-" + fmt.Sprint(i) + "-usertable-000001-Data.db")
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
			iter := sstable.GetSSTableIterator("level-" + levelstr + "-usertable-000001-Data.db")
			for entry := iter.Next(); iter.Valid; entry = iter.Next() {
				if string(entry.Key) == string(key) {
					return string(entry.Value), true
				}
			}
		} else {
			for j := 1; j < 3; j++ {
				iter := sstable.GetSSTableIterator("level-" + levelstr + "-usertable-00000" + strconv.Itoa(j) + "-Data.db")
				for entry := iter.Next(); iter.Valid; entry = iter.Next() {
					if string(entry.Key) == string(key) {
						return string(entry.Value), true
					}
				}
			}
		}

	}
	fmt.Println("Ne nalazi se ni u jednoj sstabeli niti memtable")
	return "", false
}
