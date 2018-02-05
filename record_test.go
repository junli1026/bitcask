package bitcask

import (
	"bytes"
	"fmt"
	"testing"
)

func TestSerialize(t *testing.T) {
	r := &record{
		tstamp: 20,
		key:    "name",
		value:  []byte("李浚"),
	}

	d, _ := serialize(r)
	fmt.Println(len(d))

	reader := bytes.NewReader(d)
	out, _ := deserializeFrom(reader)
	if out == nil {
		t.Fail()
	}

	if out.tstamp != r.tstamp || out.key != r.key || string(r.value) != string(out.value) {
		t.Fail()
	}
}
