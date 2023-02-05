package sstable

// Funkcija vraca bool vrednost koja kaze da li se intervali [begin1, end1] i
// [begin2, end2] seku
// Pretpostavlja se da su prosledjeni intervali validni
func RangesIntersect(begin1 string, end1 string, begin2 string, end2 string) bool {
	return (begin1 <= end2) && (end1 >= begin2)
}

func RangeScanSSTable(begin string, end string, sstFileName string, indexFilename string, summaryFilename string, filterFilename string) []*SSTableEntry {

	result := make([]*SSTableEntry, 0)

	summaryIterator := &SummaryIterator{}
	indexIterator := &IndexIterator{}
	sstableIterator := &SSTableIterator{}

	if indexFilename != "" {
		summaryIterator = GetSummaryIteratorFromFile(summaryFilename)
		indexIterator = GetIndexIteratorFromIndexFile(indexFilename)
		sstableIterator = GetSSTableIterator(sstFileName)

	} else {
		summaryIterator = GetSummaryIteratorFromSSTableFile(sstFileName)
		indexIterator = GetIndexIteratorFromSSTableFile(sstFileName)
		sstableIterator = GetSSTableIterator(sstFileName)
	}

	if summaryIterator == nil || indexIterator == nil || sstableIterator == nil {
		return result
	}

	if !RangesIntersect(summaryIterator.Begin_key, summaryIterator.End_key, begin, end) {
		return result
	}

	indexOffsetToBeginFrom := -1
	for entry := summaryIterator.Next(); summaryIterator.Valid; entry = summaryIterator.Next() {
		if RangesIntersect(entry.FirstKey, entry.LastKey, begin, end) {
			indexOffsetToBeginFrom = int(entry.Offset)
			break
		}
	}

	if indexOffsetToBeginFrom == -1 {
		return result
	}

	indexIterator.SeekToOffset(int64(indexOffsetToBeginFrom))

	indexEntry := indexIterator.Next()

	if indexEntry == nil {
		return result
	} else {
		sstableIterator.SeekToOffset(indexEntry.Offset)
	}

	for entry := sstableIterator.Next(); sstableIterator.Valid; entry = sstableIterator.Next() {
		key_str := string(entry.Key)

		// Ovde vracamo i logicki obrisane jer u Range Scan operaciji
		// u okviru sistema moramo gledati i druge tabele pa mora uzeti
		// u obzir da li je kljuc bio obrisan u nekoj drugoj tabeli
		if begin <= key_str && key_str <= end {
			result = append(result, entry)
		} else if key_str > end {
			break
		}
	}

	sstableIterator.Close()
	indexIterator.Close()
	summaryIterator.Close()
	return result
}
