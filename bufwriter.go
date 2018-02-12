package bitcask

import (
	"os"
)

type bufwriter struct {
	f    *os.File
	used int
	buf  []byte
}

func newBufWriter(f *os.File, bufsz uint32) *bufwriter {
	bw := &bufwriter{}
	bw.f = f
	bw.buf = make([]byte, bufsz, bufsz)
	bw.used = 0
	return bw
}

func (bw *bufwriter) Write(data []byte) (int, error) {
	if len(data)+bw.used <= len(bw.buf) {
		copy(bw.buf[bw.used:], data)
		bw.used += len(data)
		return len(data), nil
	}

	if bw.used > 0 {
		_, err := bw.f.Write(bw.buf[0:bw.used])
		if err != nil {
			panic(err)
		}
		bw.used = 0
	}

	if len(data) > len(bw.buf) {
		return bw.f.Write(data)
	}
	copy(bw.buf, data)
	bw.used += len(data)
	return len(data), nil
}

func (bw *bufwriter) Flush() {
	if bw.used > 0 {
		_, err := bw.f.Write(bw.buf[0:bw.used])
		if err != nil {
			panic(err)
		}
		bw.used = 0
	}
}

func (bw *bufwriter) Buffered() int {
	return bw.used
}

func (bw *bufwriter) GetBuffer() []byte {
	return bw.buf
}
