package sstable

import "strings"

// Funkcija vraca bool vrednost koja kaze da li se string koji pocinje
// prefiksom prefiks moze nalaziti u intervalu [begin, end]
func CouldPrefixBeInRange(begin string, end string, prefix string) bool {
	// Ako prefiks upada u opseg, string sa tim prefiksom moze biti u opsegu
	// Takodje ako opseg pocinje stringom koji ima trazeni prefiks, ali i
	// jos karaktera posle istog onda treba da trazimo stringove sa prefiksom
	// i u tom opsegu.
	return (begin <= prefix && prefix <= end) || (strings.HasPrefix(begin, prefix))
}

func PrefixScanSSTable(prefix string, sstFileName string, indexFilename string, summaryFilename string, filterFilename string) []*SSTableEntry {

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
	defer sstableIterator.Close()
	defer indexIterator.Close()
	defer summaryIterator.Close()

	if summaryIterator == nil || indexIterator == nil || sstableIterator == nil {
		return result
	}

	if !CouldPrefixBeInRange(summaryIterator.begin_key, summaryIterator.end_key, prefix) {
		return result
	}

	indexOffsetToBeginFrom := -1
	for entry := summaryIterator.Next(); summaryIterator.Valid; entry = summaryIterator.Next() {
		if CouldPrefixBeInRange(entry.FirstKey, entry.LastKey, prefix) {
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

		if strings.HasPrefix(key_str, prefix) {
			result = append(result, entry)
		} else if key_str > prefix {
			break // S obzirom da je tabela sortirana znamo da sledeci stringovi ne mogu pocinjati
			// trazenim prefiksom jer su leksikografski veci
		}
	}

	return result
}
