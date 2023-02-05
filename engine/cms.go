package engine
//Datoteka uvozi metode za rukovanje Count-Min-Sketch-om u engine
import "github.com/darkokos/NAiSP_Projekat/CMS"

func InitCMS(precision float64, certainty float64) *CMS.CMS{
	cms := &CMS.CMS{}
	cms.Init(precision, certainty)
	return cms
}
func AddToCMS(cms *CMS.CMS, key []byte){
	cms.Add(key)
}
func ReadFromCMS(cms *CMS.CMS, key []byte) uint{
	return cms.Read(key)
}
func SerializeCMS(cms *CMS.CMS) []byte{
	return cms.Serialize()
}
func DeserializeCMS(b []byte) CMS.CMS{
	return CMS.Deserialize(b)
}