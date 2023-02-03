package engine

import (
	"io/ioutil"
	"log"
	"os"

	wal "github.com/darkokos/NAiSP_Projekat/WAL"
)

// Cita WAL-ove i ponovo primenjuje operacije
func (engine *DB) ReplayWal() {
	_, err := ioutil.ReadDir("./wal")
	if err != nil {
		log.Println("Nije citanje wal direktorijuma. WAL nije procitan.")
	}

	/*
		for _, file := range files {
			wal.ReadWAL()
		}
	*/

	walEntries := wal.ReadWAL()

	// TODO: Ovo ne bi treblo da proizvodi WAL zapise, a proizvodi ih.
	// Dodati put-ove i delete-ove koji ne gledaju wal
	for _, walEntry := range walEntries {
		if !walEntry.Tombstone {
			engine.Put(string(walEntry.Key), walEntry.Value)
		} else {
			engine.Delete(string(walEntry.Key))
		}
	}
}

// Pravi wal direktorijum u trenutnom radnom direktorijumu ako ne postoji
func (engine *DB) CreateWalDirIfDoesNotExist() {
	_, err := os.Stat("wal")
	if os.IsNotExist(err) {
		err := os.Mkdir("wal", os.ModePerm)
		if err != nil {
			log.Println("Nije uspelo kreiranje wal direktorijuma. WAL nije procitan.")
		}
	}
}
