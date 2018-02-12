package bitcask

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
)

const (
	recordHeaderSz = 20
)

type record struct {
	tstamp int64
	key    string
	value  []byte
}

func serialize(r *record) ([]byte, error) {
	k := []byte(r.key)
	// crc32 + tstamp64 + keysize32 + valuesize32 + key + value
	sz := 4*5 + len(k) + len(r.value)
	data := make([]byte, sz)
	binary.LittleEndian.PutUint64(data[4:], uint64(r.tstamp))
	binary.LittleEndian.PutUint32(data[12:], uint32(len(k)))
	binary.LittleEndian.PutUint32(data[16:], uint32(len(r.value)))
	copy(data[20:], k)
	copy(data[20+len(k):], r.value)
	crc := crc32.ChecksumIEEE(data[4:])
	binary.LittleEndian.PutUint32(data[0:4], crc)
	return data, nil
}

func deserialize(data []byte) (*record, error) {
	crc := binary.LittleEndian.Uint32(data[0:4])

	tstamp := binary.LittleEndian.Uint64(data[4:12])
	keySz := binary.LittleEndian.Uint32(data[12:16])
	valSz := binary.LittleEndian.Uint32(data[16:20])
	hsz := uint32(recordHeaderSz)

	if crc != crc32.ChecksumIEEE(data[4:hsz+keySz+valSz]) {
		// log error
		return nil, errors.New("crc mistatch")
	}

	r := &record{}
	r.tstamp = int64(tstamp)
	r.key = string(data[hsz : hsz+keySz])
	r.value = data[hsz+keySz : hsz+keySz+valSz]
	return r, nil
}

func deserializeFrom(reader io.Reader) (*record, error) {
	header := make([]byte, recordHeaderSz)
	n, err := io.ReadFull(reader, header)

	if err != nil {
		// log error
		n++
		return nil, err
	}

	crc := binary.LittleEndian.Uint32(header[0:4])
	tstamp := binary.LittleEndian.Uint64(header[4:12])
	keySz := binary.LittleEndian.Uint32(header[12:16])
	valSz := binary.LittleEndian.Uint32(header[16:20])

	data := make([]byte, uint32(recordHeaderSz)+keySz+valSz)
	copy(data, header)
	n, err = io.ReadFull(reader, data[recordHeaderSz:])
	if err != nil {
		// log error
		return nil, err
	}

	if crc != crc32.ChecksumIEEE(data[4:]) {
		// log error
		return nil, errors.New("crc mistatch")
	}

	r := &record{}
	r.tstamp = int64(tstamp)
	hSz := uint32(recordHeaderSz)
	r.key = string(data[hSz : hSz+keySz])
	r.value = data[hSz+keySz : hSz+keySz+valSz]
	return r, nil
}
