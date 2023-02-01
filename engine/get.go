package engine

func (engine *DB) Get(key string) []byte {
	key_bytes := []byte(key)
	val, ok := engine.lsm_tree.Get(key_bytes)

	if !ok {
		return nil
	} else {
		val_bytes := []byte(val)
		engine.cache.Add(key_bytes, val_bytes)
		return val_bytes
	}

}
