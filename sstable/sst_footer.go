package sstable

import (
	"encoding/binary"
	"io"
	"os"
)

const (
	SST_FOOTER_SIZE = 4 * 8
)

type SSTFooter struct {
	IndexOffset    int64
	SummaryOffset  int64
	FilterOffset   int64
	MetadataOffset int64
}

// Cita footer iz sst fajla ili vraca nil ako je doslo do greske
// Deskriptor fajla ce biti postavljen na poziciju iza footer-a ako je citanje uspesno
func ReadSSTFooter(f *os.File) *SSTFooter {
	footer := &SSTFooter{}
	_, err := f.Seek((SST_FOOTER_SIZE+SSTABLE_MAGIC_NUMBER_SIZE)*-1, io.SeekEnd)
	if err != nil {
		return nil
	}

	err = binary.Read(f, binary.LittleEndian, footer)
	if err != nil {
		return nil
	}

	return footer
}
