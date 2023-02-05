package sstable

// Ova funkcija se ne koristi - Implementirano u okviru SSTFileWriter-a
// Pise Merkle stablo u zaseban fajl
/*
func writeMetadataSeparateFile(entries []*memtable.MemTableEntry, filename string) {
	data := make([][]byte, len(entries))

	for i, entry := range entries {
		data[i] = make([]byte, len(entry.Value))
		copy(data[i], entry.Value)
	}

	metadata := merkleTree.CreateMerkleTree(data)

	metadata_bytes := merkleTree.SerializeTree(metadata)

	metdata_file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		fmt.Println("Greska u otvaranju fajla za merkle stablo")
		return
	}

	err = binary.Write(metdata_file, binary.LittleEndian, metadata_bytes)
	if err != nil {
		fmt.Println("Greska u zapsivanju merkle stabla")
		return
	}

	metdata_file.Close()
}
*/
