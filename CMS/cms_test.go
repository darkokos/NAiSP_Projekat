package CMS
import "testing"
import "fmt"
func Test(t *testing.T){
	cms := CMS{}
	cms.Init(0.9, 0.9)
	cms.Add([]byte{1, 2})
	cms.Add([]byte{1, 2})
	cms.Add([]byte{1, 2})
	fmt.Print(cms.Read([]byte{1, 2}))
}