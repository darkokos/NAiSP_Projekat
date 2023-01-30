package sstable

import "os"

const (
	SST_FOOTER_SIZE = 4 * 8
)

type SSTFooter struct {
	IndexOffset    int64
	SummaryOffset  int64
	FilterOffset   int64
	MetadataOffset int64
}

func tryReadSSTFooter(f *os.File) {

}
