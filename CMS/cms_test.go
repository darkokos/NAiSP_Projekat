package CMS

import (
	"fmt"
	"testing"
)

func Test(t *testing.T) {
	cms := CMS{}
	cms.Init(0.9, 0.9)
	cms.Add([]byte{1, 2})
	cms.Add([]byte{1, 2})
	cms.Add([]byte{1, 2})
	fmt.Print(cms.Serialize())
	fmt.Print(cms.Read([]byte{1, 2}))
	cms2 := Deserialize(cms.Serialize())
	fmt.Print("\n", cms2.Serialize())
	fmt.Print(cms2.Read([]byte{1, 2}))
}
