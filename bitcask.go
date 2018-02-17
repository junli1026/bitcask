package bitcask

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
	MaxDataSize    = 1024 * 1024
	dataFileFormat = "%d.dat"
	hintFileFormat = "%d.hint"
	activeFile     = "active.dat"
	tombstone      = "\r\n"
)

func check(err error) {
	if err != nil {
		panic(err)
	}
}

// Bitcask for bitcask structure
type Bitcask struct {
	dir      string
	bufSz    uint32
	rwlock   sync.RWMutex
	keydir   *keydir
	activeID int
	buckets  []*os.File
	writer   *bufwriter
}

// Open for creating a bitcask
func Open(dir string, bufSz uint32) *Bitcask {
	var err error
	bc := &Bitcask{}
	bc.bufSz = bufSz
	bc.keydir = newKeydir()
	bc.buckets = make([]*os.File, 256, 256)
	bc.dir, err = filepath.Abs(dir)
	check(err)

	var fileinfos []os.FileInfo
	fileinfos, err = ioutil.ReadDir(bc.dir)
	check(err)

	var active *os.File
	filesMap := make(map[int]string)
	maxID := -1
	for _, info := range fileinfos {
		if info.IsDir() {
			continue
		}
		fullname := info.Name()
		ext := filepath.Ext(fullname)
		name := fullname[0 : len(fullname)-len(ext)]

		if fullname == activeFile {
			active, err = os.OpenFile(filepath.Join(bc.dir, fullname), os.O_APPEND|os.O_RDWR, 0600)
			check(err)
		} else if ext == ".dat" || ext == ".hint" {
			id, err := strconv.Atoi(name)
			if err != nil {
				continue
			}
			if id > maxID {
				maxID = id
			}
			if filesMap[id] != "" && ext == ".dat" {
				continue
			} else {
				filesMap[id] = filepath.Join(bc.dir, fullname)
			}
		}
	}

	if active == nil {
		active, err = os.OpenFile(filepath.Join(bc.dir, activeFile), os.O_APPEND|os.O_RDWR|os.O_CREATE, 0600)
		check(err)
	}

	bc.activeID = maxID + 1
	bc.writer = newBufWriter(active, bufSz)
	bc.buckets[bc.activeID] = active
	bc.keydir, err = loadFromData(bc.activeID, active)
	check(err)

	ids := make([]int, 0)
	for i := range filesMap {
		ids = append(ids, i)
	}
	sort.Ints(ids)

	for i := len(ids) - 1; i >= 0; i-- {
		id := ids[i]
		path := filesMap[id]
		f, err := os.OpenFile(path, os.O_RDONLY, 0600)
		check(err)
		bc.buckets[id] = f

		var loaderr error
		var kd *keydir
		if strings.HasSuffix(path, ".dat") {
			kd, loaderr = loadFromData(id, bc.buckets[i])
		} else {
			kd, loaderr = loadFromHint(id, bc.buckets[i])
		}
		check(loaderr)
		bc.keydir.merge(kd)
	}
	return bc
}

// Get return the value
func (bc *Bitcask) Get(key string) ([]byte, error) {
	bc.rwlock.RLock()
	defer bc.rwlock.RUnlock()

	e := bc.keydir.table[key]
	if e == nil {
		return nil, nil
	}

	var f *os.File
	var info os.FileInfo
	var err error

	f = bc.buckets[e.fid]
	info, err = f.Stat()
	check(err)
	offset := e.valpos - info.Size()
	if offset >= 0 {
		buf := bc.writer.GetBuffer()
		r, err := deserialize(buf[offset:])
		if err != nil {
			return nil, err
		}
		return r.value, nil
	}
	return bc.keydir.get(f, key)
}

func (bc *Bitcask) set(key string, value []byte) error {
	var active, data *os.File
	active = bc.buckets[bc.activeID]
	fileinfo, err := active.Stat()
	check(err)
	if int64(bc.writer.Buffered())+fileinfo.Size() > int64(MaxDataSize) {
		bc.writer.Flush()
		active.Close()
		oldname := filepath.Join(bc.dir, activeFile)
		newname := filepath.Join(bc.dir, fmt.Sprintf(dataFileFormat, bc.activeID))
		os.Rename(oldname, newname)

		data, err = os.OpenFile(newname, os.O_RDONLY, 0600)
		check(err)
		bc.buckets[bc.activeID] = data

		active, err = os.OpenFile(filepath.Join(bc.dir, activeFile), os.O_APPEND|os.O_RDWR|os.O_CREATE, 0600)
		check(err)
		bc.activeID++
		bc.buckets[bc.activeID] = active
		bc.writer = newBufWriter(active, bc.bufSz)
	}
	return bc.keydir.put(bc.activeID, active, bc.writer, key, value)
}

// Put set the value
func (bc *Bitcask) Put(key string, value []byte) error {
	bc.rwlock.Lock()
	defer bc.rwlock.Unlock()
	return bc.set(key, value)
}

// Delete for delete an entry
func (bc *Bitcask) Delete(key string) error {
	bc.rwlock.Lock()
	defer bc.rwlock.Unlock()
	err := bc.set(key, []byte(tombstone))
	if err != nil {
		return err
	}
	delete(bc.keydir.table, key)
	return nil
}

// Close for close bc
func (bc *Bitcask) Close() {
	bc.rwlock.Lock()
	defer bc.rwlock.Unlock()

	bc.writer.Flush()
	for _, f := range bc.buckets {
		if f != nil {
			f.Close()
		}
	}
}
