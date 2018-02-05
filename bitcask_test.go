package bitcask

import (
	"fmt"
	"strconv"
	"testing"
)

func TestOpen(t *testing.T) {
	bc := Open("./")
	defer bc.Close()

	for i := 0; i < 100000; i++ {
		key := strconv.Itoa(i)
		value := "value_" + key
		go bc.Put(key, []byte(value))
	}

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
