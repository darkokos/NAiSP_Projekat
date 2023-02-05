package engine

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"

	"github.com/darkokos/NAiSP_Projekat/config"
)

func TestSTCS(t *testing.T) {
	// Dodaje 6000 random kljuceva duzine 100 u bazu i proverava da li svi mogu da se uzmu nakon
	// kompakcije
	config.DefaultConfiguration.MemtableSize = 200
	config.DefaultConfiguration.MemtableStructure = "b_tree"
	config.DefaultConfiguration.CompactionStrategy = "size_tiered"
	config.DefaultConfiguration.SummaryDensity = 50
	config.ReadConfig()
	Cleanup()
	defer Cleanup()

	db := GetNewDB()

	number_of_elements := 6000
	elements := make([]string, 0, number_of_elements)
	for i := 0; i < number_of_elements; i++ {

		length := 100

		ran_str := make([]byte, length)

		// Generating Random string
		for i := 0; i < length; i++ {
			ran_str[i] = byte(65 + rand.Intn(25))
		}

		elements = append(elements, string(ran_str))

		value := make([]byte, 0, 4)
		binary.LittleEndian.AppendUint32(value, uint32(i))

		db.Put(string(ran_str), value)

		if i%1000 == 0 {
			fmt.Println(i, "/", number_of_elements)
		}
	}

	db.RunCompaction()

	for _, key := range elements {
		if db.Get(key) == nil {
			t.Fatalf("Ovo je trebalo da nadje")
		}
	}

}

func TestLCS(t *testing.T) {
	// Dodaje 6000 random kljuceva duzine 100 u bazu i proverava da li svi mogu da se uzmu nakon
	// kompakcije
	config.DefaultConfiguration.MemtableSize = 200
	config.DefaultConfiguration.MemtableStructure = "b_tree"
	config.DefaultConfiguration.CompactionStrategy = "leveled"
	config.DefaultConfiguration.SummaryDensity = 50
	config.ReadConfig()
	Cleanup()
	defer Cleanup()

	db := GetNewDB()

	number_of_elements := 6000
	elements := make([]string, 0, number_of_elements)
	for i := 0; i < number_of_elements; i++ {

		length := 100

		ran_str := make([]byte, length)

		// Generating Random string
		for i := 0; i < length; i++ {
			ran_str[i] = byte(65 + rand.Intn(25))
		}

		elements = append(elements, string(ran_str))

		value := make([]byte, 0, 4)
		binary.LittleEndian.AppendUint32(value, uint32(i))

		db.Put(string(ran_str), value)

		if i%1000 == 0 {
			fmt.Println(i, "/", number_of_elements)
		}
	}

	db.RunCompaction()

	for _, key := range elements {
		if db.Get(key) == nil {
			t.Fatalf("Ovo je trebalo da nadje")
		}
	}

}
