package sstable

import "os"

const (
	SST_FOOTER_SIZE = 4 * 8
)

type SSTFooter struct {
	indexOffset    int64
	summaryOffset  int64
	filterOffset   int64
	metadataOffset int64
}

func tryReadSSTFooter(f *os.File) {

}
