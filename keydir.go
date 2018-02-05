package bitcask

import (
	"bufio"
	"io"
	"os"
	"syscall"
	"time"
)

type entry struct {
	fid       int
	valsz     uint32
	valpos    int64
	timestamp int64
}

type keydir struct {
	table map[string]*entry
}

func newKeydir() *keydir {
	return &keydir{
		table: make(map[string]*entry),
	}
}

func loadFromData(fid int, f *os.File) (*keydir, error) {
	kd := newKeydir()
	r := bufio.NewReader(f)
	pos := int64(0)
	for {
		record, err := deserializeFrom(r)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		key := record.key
		value := record.value
		kd.table[key] = &entry{
			fid:       fid,
			valsz:     uint32(len(value)),
			valpos:    pos,
			timestamp: time.Now().Unix(),
		}
		pos += int64(recordHeaderSz) + int64(len([]byte(key))) + int64(len(value))
	}
	return kd, nil
}

func loadFromHint(fid int, f *os.File) (*keydir, error) {
	return nil, nil
}

func (kd *keydir) get(f *os.File, key string) ([]byte, error) {
	entry := kd.table[key]
	if entry == nil {
		return nil, nil
	}

	buf := make([]byte, recordHeaderSz+len([]byte(key))+int(entry.valsz))

	n, err := syscall.Pread(int(f.Fd()), buf, entry.valpos)
	if n != len(buf) {
		// log err
		return nil, nil
	}

	var r *record
	r, err = deserialize(buf)
	if err != nil {
		return nil, err
	}
	return r.value, err
}

func (kd *keydir) put(fid int, f *os.File, writer *bufio.Writer, key string, value []byte) error {
	r := &record{
		tstamp: 0,
		key:    key,
		value:  value,
	}

	block, err := serialize(r)
	if err != nil {
		// log err
		return err
	}

	var fileInfo os.FileInfo
	var pos int64

	if fileInfo, err = f.Stat(); err != nil {
		return err
	}

	pos = fileInfo.Size() + int64(writer.Buffered())
	if _, err = writer.Write(block); err != nil {
		return err
	}
	writer.Flush()

	kd.table[key] = &entry{
		fid:       fid,
		valsz:     uint32(len(value)),
		valpos:    pos,
		timestamp: time.Now().Unix(),
	}
	return nil
}

func (kd *keydir) merge(other *keydir) {
	for k, entry := range other.table {
		if kd.table[k] == nil {
			kd.table[k] = entry
		}
	}
}
