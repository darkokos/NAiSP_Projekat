package compactions

import (
	"fmt"
	"math"
	"path/filepath"

	lsmtree "github.com/darkokos/NAiSP_Projekat/LSMTree"
	"github.com/darkokos/NAiSP_Projekat/config"
)

func LCS() {
	for level := 1; level <= lsmtree.Findlevel() && level < int(config.Configuration.LSMTreeLevels); level++ { //Prolazi se kroz sve nivoe LSM stabla sem poslednjeg, jer se on ne kompaktuje, radi korektnog lancanog kompaktovanja
		tables_to_merge, err := filepath.Glob("level-" + fmt.Sprintf("%02d", level) + "-usertable-*-Data.db") //Izdvajanje svih tabela trenutnog nivoa, za kompaktovanje
		if err != nil {
			panic(err)
		}

		num_of_tables := len(tables_to_merge)

		if num_of_tables < int(math.Pow10(level)) { //Kraj algoritma ako na trenutnom nivou nema 10 * broj tabela prethodnog nivoa
			break
		}

		lsmtree.MergeMultipleTablesLCS(tables_to_merge, level+1) //Poseban algoritam za merge koji ce otvarati novu tabelu svaki put kada se napise 160 zapisa u trenutnu, pocevsi od prosledjene tabele. Potrebno ga je napisati u implementaciji LSM stabla, modifikacijom STCS merge algoritma.
	}
}
