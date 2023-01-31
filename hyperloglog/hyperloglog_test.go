package hyperloglog

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"
)

func TestHyperLogLog(t *testing.T) {
	hll := CreateHLL(12)
	rand.Seed(time.Now().Unix())
	number_of_elements := 100000
	for i := 0; i < number_of_elements; i++ {

		length := 10000

		ran_str := make([]byte, length)

		// Generating Random string
		for i := 0; i < length; i++ {
			ran_str[i] = byte(65 + rand.Intn(25))
		}

		//fmt.Println("String")
		//fmt.Println(string(ran_str))

		hll.Add(ran_str)
	}
	/*
		for i:=0; i < 1000000; i++ {
			hll.add_simulation(rand.Uint64())
		}*/

	fmt.Println("Broj clanova")
	estimate := hll.Estimate()
	fmt.Println(hll.Estimate())

	if math.Abs(float64(number_of_elements)-estimate) > float64(number_of_elements)/20 {
		t.Fatalf("Broj clanova nije dobar")
	}

	fmt.Println("M")
	fmt.Println(hll.m)
}
