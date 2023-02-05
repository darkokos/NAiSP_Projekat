package engine

import (
	"testing"

	"github.com/darkokos/NAiSP_Projekat/config"
)

func TestPrefixScan(t *testing.T) {
	config.DefaultConfiguration.MemtableSize = 10
	config.DefaultConfiguration.RateLimit = 9999
	config.DefaultConfiguration.MultipleFileSSTable = true
	config.ReadConfig()
	Cleanup()

	db := GetNewDB()

	db.Put("MaxTemp/Novi Sad/2023-02-05", []byte{1})
	db.Put("MaxTemp/Belgrade/2023-02-05", []byte{0})
	db.Put("Humidity/Novi Sad/2023-02-05", []byte{3})
	db.Put("Humidity/Belgrade/2023-02-05", []byte{4})

	db.Put("MaxTemp/Novi Sad/2023-02-06", []byte{1})
	db.Put("MaxTemp/Belgrade/2023-02-06", []byte{1})
	db.Put("Humidity/Novi Sad/2023-02-06", []byte{5})
	db.Put("Humidity/Belgrade/2023-02-06", []byte{3})

	db.Put("MaxTemp/Novi Sad/2023-02-07", []byte{1})
	db.Put("MaxTemp/Belgrade/2023-02-07", []byte{2})
	db.Put("Humidity/Novi Sad/2023-02-07", []byte{5})
	db.Put("Humidity/Belgrade/2023-02-07", []byte{5})

	db.Put("MaxTemp/Novi Sad/2023-07-07", []byte{31})
	db.Put("MaxTemp/Belgrade/2023-07-07", []byte{32})
	db.Put("Humidity/Novi Sad/2023-07-07", []byte{1})
	db.Put("Humidity/Belgrade/2023-07-07", []byte{0})

	max_temps_ns := db.List("MaxTemp/Novi Sad", 1, 6)
	if len(max_temps_ns) != 4 || max_temps_ns[0][0] != 1 || max_temps_ns[1][0] != 1 || max_temps_ns[2][0] != 1 || max_temps_ns[3][0] != 31 {
		t.Fatalf("Nesto se nije dobro procitalo")
	}

	// Trazimo vlaznost vazduha za februar
	hum_bg := db.List("Humidity/Belgrade/2023-02", 1, 6)
	if len(hum_bg) != 3 || hum_bg[0][0] != 4 || hum_bg[1][0] != 3 || hum_bg[2][0] != 5 {
		t.Fatalf("Nesto se nije dobro procitalo")
	}

	Cleanup()
}

// TODO: Test sa nevalidnim parametrima
// TODO: Test gde se ne pojavljuje ni jedan rezulata
// TODO: Test gde trazimo ceo string kao prefiks
// TODO: Test interakcije sa promenama i brisanjima
