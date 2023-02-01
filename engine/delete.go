package engine

func (engine *DB) Delete(key string) bool {
	//TODO: Delete operacija

	ok := engine.memtable.Delete(key)
	if ok {
		engine.cache.Edit([]byte(key), nil) // Moramo ukloniti element iz kesa - prevencija zastarelog kesa
		return true
	} else {
		return false
	}
}
