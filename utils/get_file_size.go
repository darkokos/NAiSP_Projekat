package utils

import "os"

// Funkcija vraca velicinu fajla sa imenom filename ili -1 ako je doslo do greske
func GetFileSize(filename string) int64 {
	stat, err := os.Stat(filename)
	if err != nil {
		return -1
	}

	return stat.Size()
}
