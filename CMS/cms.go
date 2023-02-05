package CMS

import (
	"encoding/binary"
	"math"
)

type CMS struct {
	table     [][]uint
	k         uint
	m         uint
	precision float64
	certainty float64
	hashes    []HashWithSeed
}
//Funkcija za inicijalizaciju nad vec napravljenim cms objektom, uzima preciznost i sigurnost a k i m racuna
func (cms *CMS) Init(precision float64, certainty float64) {
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
//Funkcija za dodavanje u cms
func (cms *CMS) Add(key []byte) {
	var j uint64
	for i := range (*cms).hashes {
		j = (*cms).hashes[i].Hash(key) % (uint64)((*cms).m)
		(*cms).table[i][j] += 1
	}
}
//Funkcija za citanje iz cms-a
func (cms *CMS) Read(key []byte) uint {
	min := ^uint(0)
	var j uint64
	for i := range (*cms).hashes {
		j = (*cms).hashes[i].Hash(key) % (uint64)((*cms).m)
		if (*cms).table[i][j] < min {
			min = (*cms).table[i][j]
		}
	}
	return min
}
//Funkcija za serijalizaciju cms-a
func (cms *CMS) Serialize() []byte {
	if cms == nil{
		return nil
	}
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
//Funkcija za deserijalizaciju cms-a

func Deserialize(buf []byte) *CMS {
	if buf == nil || len(buf) == 0{
		return nil
	}
	precision := deserializeFloat(buf[0:8])
	certainty := deserializeFloat(buf[8:16])
	cms := CMS{}
	cms.Init(precision, certainty)
	x := 16
	for i := range cms.table {
		for j := range cms.table[i] {

			cms.table[i][j] = uint(deserializeUint(buf[x : x+4]))
			x += 4
		}
	}
	return &cms
}
//Pomocna funkcije za serijalizaciju i deserijalizaciju uint-a i float-a
func serializeUint(x uint32) []byte {
	a := make([]byte, 4)
	binary.LittleEndian.PutUint32(a, x)
	return a
}
func serializeFloat(f float64) []byte {
	ret := make([]byte, 8)
	binary.LittleEndian.PutUint64(ret, math.Float64bits(f))
	return ret
}
func deserializeFloat(buf []byte) float64 {
	return math.Float64frombits(binary.LittleEndian.Uint64(buf))
}
func deserializeUint(buf []byte) uint32 {
	return binary.LittleEndian.Uint32(buf)
}
