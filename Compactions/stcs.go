package compactions

import (
	"fmt"
	"path/filepath"
	"time"

	lsmtree "github.com/darkokos/NAiSP_Projekat/LSMTree"
	"github.com/darkokos/NAiSP_Projekat/config"
)

func STCS() {
	for level := 1; level <= lsmtree.Findlevel() && level < int(config.Configuration.LSMTreeLevels); level++ { //Prolazi se kroz sve nivoe LSM stabla sem poslednjeg, jer se on ne kompaktuje, radi korektnog lancanog kompaktovanja
		tables_to_merge, err := filepath.Glob("level-" + fmt.Sprintf("%02d", level) + "-usertable-*-Data.db") //Izdvajanje svih tabela trenutnog nivoa, za kompaktovanje
		if err != nil {
			panic(err)
		}

		num_of_tables := len(tables_to_merge)

		if num_of_tables < 2 { //Kraj algoritma ako ne postoje barem 2 tabele za kompakciju
			break
		}

		result_table_name := "level-" + fmt.Sprintf("%02d", level+1) + "-usertable-" + fmt.Sprintf("%020d", time.Now().UnixNano()) + "-Data.db" //Naziv tabele na sledecem nivou koja ce se dobiti kao rezultat kompaktovanja

		lsmtree.MergeMultipleTables(tables_to_merge, result_table_name)
	}
}
