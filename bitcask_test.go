package bitcask

import (
	"fmt"
	"strconv"
	"testing"
)

func TestOpen(t *testing.T) {
	bc := Open("./", 0)
	for i := 0; i < 100000; i++ {
		key := strconv.Itoa(i)
		value := "value_" + key
		bc.Put(key, []byte(value))
	}
	bc.Close()

	bc = Open("./", 0)
	defer bc.Close()
	c := 0
	for i := 0; i < 100000; i++ {
		key := strconv.Itoa(i)
		v, _ := bc.Get(key)

		if v == nil {
			c++
			continue
		}
		if string(v) != "value_"+key {
			t.Fail()
		}
	}
	fmt.Println(strconv.Itoa(c))
}

func BenchmarkPutGet100000(b *testing.B) {
	bc := Open("./", 0)
	defer bc.Close()

	for i := 0; i < 100000; i++ {
		key := strconv.Itoa(i)
		value := "value_" + key
		bc.Put(key, []byte(value))
	}
	b.ResetTimer()

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		key := strconv.Itoa(i)
		bc.Get(key)
	}
	b.StopTimer()
}
