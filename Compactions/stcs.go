package stcs

import (
	"fmt"

	lsmtree "github.com/darkokos/NAiSP_Projekat/LSMTree"
	"github.com/darkokos/NAiSP_Projekat/sstable"
)

func STCS() {
	for level := 1; level < lsmtree.Findlevel(); level++ { //Prolazi se kroz sve nivoe LSM stabla sem poslednjeg, jer se on ne kompaktuje, radi korektnog lancanog kompaktovanja
		num_of_tables := 0
		tables_to_merge := make([]string, 0)
		for { //Prebrojavanje tabela na trenutnom nivou
			table_name := "level-" + fmt.Sprintf("%02d", level) + "-usertable-" + fmt.Sprintf("%06d", num_of_tables+1) + "--Data.db"
			iter := sstable.GetSSTableIterator(table_name)
			if iter == nil {
				break
			}

			tables_to_merge = append(tables_to_merge, table_name)
			num_of_tables++
		}

		if num_of_tables < 2 { //Kraj algoritma ako ne postoje barem 2 tabele za kompakciju
			break
		}

		num_of_tables = 0
		result_table_name := ""
		for { //Nalazenje korektnog naziva tabele koja ce se formirati na sledecem nivou, kao rezultat kompakcije trenutnog nivoa
			result_table_name = "level-" + fmt.Sprintf("%02d", level+1) + "-usertable-" + fmt.Sprintf("%06d", num_of_tables+1) + "--Data.db"
			iter := sstable.GetSSTableIterator(result_table_name)
			if iter == nil {
				break
			}

			num_of_tables++
		}

		lsmtree.MergeMultipleTables(tables_to_merge, result_table_name)
	}
}
