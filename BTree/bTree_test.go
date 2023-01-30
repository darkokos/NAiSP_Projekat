package BTree

import "testing"
import "fmt"
func Test(t *testing.T){
	tr := BTree{}
	tr.Init(3)
    tr.AddKey([]byte("2"), []byte{0, 0, 0, 2})
    tr.AddKey([]byte("3"), []byte{0, 0, 0, 3})
    tr.AddKey([]byte("1"), []byte{0, 0, 0, 1})
    tr.AddKey([]byte("4"), []byte{0, 0, 0, 4})

    fmt.Println("Dodao 4")

    ok, v := tr.GetValue([]byte("1"))
    if ok == -1 {
        t.Fatalf("Kljuc 1 bi trebalo da postoji")
    } else if v[3] != 1 {
        fmt.Println(v)
        t.Fatalf("Kljuc 1 je nadjen ali vrednost nije dobro iscitana %d", v[3])
    }

    tr.ModifyKey([]byte("1"), []byte{0, 0, 0, 10}) // Menjamo 1
    tr.AddKey([]byte("22"), []byte{0, 0, 0, 2})    // Proizovdi indeks out of range

    ok, v = tr.GetValue([]byte("1"))
    if ok == -1 {
        t.Fatalf("Kljuc 1 bi trebalo da postoji")
    } else if v[3] != 10 {
        fmt.Println(v)
        t.Fatalf("Kljuc 1 je nadjen ali vrednost nije dobro iscitana %d", v[3])
    }

    fmt.Print("TESTING")
}