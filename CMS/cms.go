package CMS
import "math"
import "encoding/binary"
type CMS struct {
	table [][] uint
	k uint
	m uint
	precision float64
	certainty float64
	hashes []HashWithSeed
}

func (cms *CMS) Init(precision float64, certainty float64){
	(*cms).precision = precision
	(*cms).certainty = certainty
	(*cms).k = CalculateK(certainty)
	(*cms).m = CalculateM(precision)
	(*cms).table = make([][]uint, (*cms).k)
	for i := range (*cms).table {
		(*cms).table[i] = make([]uint, (*cms).m)
	}
	(*cms).hashes = CreateHashFunctions((*cms).k)
	
}
func (cms *CMS) Add(key []byte){
	var j uint64
	for i := range (*cms).hashes {
		j = (*cms).hashes[i].Hash(key) % (uint64)((*cms).m)
		(*cms).table[i][j] += 1
	}
}
func (cms *CMS) Read(key []byte) uint{
	min := ^uint(0)
	var j uint64
	for i := range (*cms).hashes {
		j = (*cms).hashes[i].Hash(key) % (uint64)((*cms).m)
		if ((*cms).table[i][j] < min){
			min = (*cms).table[i][j]
		}
	}
	return min
}
func (cms *CMS) Serialize() []byte{
	var ret []byte 
	
	ret = append(ret, serializeFloat((*cms).precision)...)
	ret = append(ret, serializeFloat((*cms).certainty)...)

	for i := range (*cms).table {
		for j := range (*cms).table[i] {
			ret = append(ret, serializeUint((uint32)((*cms).table[i][j]))...)
		}
	}

	return ret
} 
func Deserialize(buf [] byte) CMS{
	precision := deserializeFloat(buf[0:8])
	certainty := deserializeFloat(buf[8:16])
	cms := CMS{}
	cms.Init(precision, certainty)
	x := 16
	for i := range cms.table {
		for j := range cms.table[i] {
			
			cms.table[i][j] = uint(deserializeUint(buf[x:x+4]))
			x += 4
		}
	}
	return cms
}
func serializeUint(x uint32) []byte{
	a := make([]byte, 4)
	binary.LittleEndian.PutUint32(a, x)
	return a
}
func serializeFloat(f float64) []byte{
    ret := make([]byte, 8)
    binary.LittleEndian.PutUint64(ret, math.Float64bits(f))
    return ret
}
func deserializeFloat(buf []byte) float64{
    return math.Float64frombits(binary.LittleEndian.Uint64(buf))
}
func deserializeUint(buf []byte) uint32{
	return binary.LittleEndian.Uint32(buf)
}