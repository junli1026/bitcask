package bitcask

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"testing"
)

func TestPutGet(t *testing.T) {
	keydir := newKeydir()
	active, _ := os.OpenFile("active.dat", os.O_APPEND|os.O_RDWR|os.O_CREATE, 0600)
	defer func() {
		active.Close()
		os.Remove("active.dat")
	}()

	writer := newBufWriter(active, 0)

	for i := 0; i < 100000; i++ {
		key := strconv.Itoa(i)
		value := "value_" + key
		keydir.put(0, active, writer, key, []byte(value))
	}
	for i := 0; i < 100000; i++ {
		key := strconv.Itoa(i)
		v, _ := keydir.get(active, key)
		if string(v) != "value_"+key {
			t.Fail()
		}
	}
}

func TestLoadFromData(t *testing.T) {
	keydir := newKeydir()
	active, _ := os.OpenFile("active.dat", os.O_APPEND|os.O_RDWR|os.O_CREATE, 0600)
	defer func() {
		active.Close()
		os.Remove("active.dat")
	}()

	writer := newBufWriter(active, 0)
	num := 100000

	for i := 0; i < num; i++ {
		key := strconv.Itoa(i)
		value := "value_" + key
		err := keydir.put(0, active, writer, key, []byte(value))
		if err != nil {
			fmt.Println("error")
		}
	}

	active.Seek(0, 0)
	kd, _ := loadFromData(0, active)
	keys := make([]int, 0, len(kd.table))
	for k := range kd.table {
		i, _ := strconv.Atoi(k)
		keys = append(keys, i)
	}
	sort.Ints(keys)

	if len(keys) != num {
		fmt.Println(len(keys))
		t.Fail()
	}

	for i := 0; i < num; i++ {
		if keys[i] != i {
			t.Fail()
		}
		k := strconv.Itoa(i)
		v, _ := keydir.get(active, k)
		if string(v) != "value_"+k {
			t.Fail()
		}
	}
}
