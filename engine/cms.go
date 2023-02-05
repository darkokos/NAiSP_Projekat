package engine
//Datoteka implementira metode za rukovanje cms-om putem kljuceva iz baze
import "github.com/darkokos/NAiSP_Projekat/CMS"

//Dodavanje CMS-a u bazu
func (engine *DB) PutCMS(key string, cms CMS.CMS) bool{
	key = "cms." + key
	return engine.Put(key, cms.Serialize())
} 
//Citanje cms-a iz baze
func  (engine *DB) GetCMS(cmsKey string) *CMS.CMS{
	return CMS.Deserialize(engine.Get("cms." + cmsKey))
}
//Dodavanje u cms koji je u bazi, putem kljuca
func  (engine *DB) AddToCMS(cmsKey string, key []byte) int{
	cms := engine.GetCMS(cmsKey)
	if cms == nil{
		return -1
	}
	cms.Add(key)
	engine.PutCMS(cmsKey, *cms)
	return 0
}
//Citanje iz cms-a koji je u bazi, putem kljuca, vraca se statusna promenljiva tipa int i promenljiva tipa uint koja je povratna vrednost cms-a
func (engine *DB) ReadFromCMS(cmsKey string, key []byte) (int, uint){
	cms := engine.GetCMS(cmsKey)
	if cms == nil{
		return -1, 0
	}
	return 0, cms.Read(key)
}
