package memtable

import (
	"fmt"
	"testing"
)

func TestMemtable(t *testing.T) {
	fmt.Println("Kompajlirao se!")

	//memTable := createMemTableFromConfig()

	memTable := makeHashMapMemTable(5)
	//fmt.Println(memTable.data)

	memTable.Update("2", []byte{0, 0, 0, 2})
	memTable.Update("3", []byte{0, 0, 0, 3})
	memTable.Update("1", []byte{0, 0, 0, 1})
	memTable.Update("4", []byte{0, 0, 0, 4})
	memTable.Update("1", []byte{0, 0, 0, 10})
	memTable.Update("5", []byte{0, 0, 0, 5})

	fmt.Println("Flush2")
	memTable.Update("5", []byte{0, 0, 0, 2})
	memTable.Update("36", []byte{0, 0, 0, 3})
	memTable.Update("231", []byte{0, 0, 0, 1})
	memTable.Update("33", []byte{0, 0, 0, 4})
	fmt.Println("Brisanje neuspesno", memTable.Delete("37"))
	fmt.Println("Brisanje uspesno", memTable.Delete("33"))
	memTable.Update("11", []byte{0, 0, 0, 10})
	//memTable.Update("5", []byte{0,0,0,5})
}
