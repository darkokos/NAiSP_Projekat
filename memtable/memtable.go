package memtable

type MemTable interface {
	Get(key string) ([]byte, bool)
	Update(key string, value []byte) bool
	Delete(key string) bool
	Flush()
}
