package bitcask

import (
	"bufio"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

func check(err error) {
	if err != nil {
		panic(err)
	}
}

// Bitcask for bitcask structure
type Bitcask struct {
	active  *os.File
	rwlock  sync.RWMutex
	keydir  *keydir
	buckets []*os.File
	writer  *bufio.Writer
}

var dataFileFormat = "%d.dat"
var hintFileFormat = "%d.hint"
var activeFile = "active.dat"

// Open for creating a bitcask
func Open(dir string) *Bitcask {
	bc := &Bitcask{}
	bc.keydir = newKeydir()
	bc.buckets = make([]*os.File, 256, 256)

	absdir, _ := filepath.Abs(dir)
	files, err := ioutil.ReadDir(absdir)
	check(err)

	filesMap := make(map[int]string)
	for _, f := range files {
		if f.IsDir() {
			continue
		}
		fullname := f.Name()
		ext := filepath.Ext(fullname)
		name := fullname[0 : len(fullname)-len(ext)]

		if fullname == activeFile {
			active, err := os.OpenFile(filepath.Join(absdir, fullname), os.O_APPEND|os.O_RDWR, 0600)
			check(err)
			bc.active = active
			bc.writer = bufio.NewWriterSize(bc.active, 200*1024*1024)
		} else if ext == ".dat" || ext == ".hint" {
			id, err := strconv.Atoi(name)
			check(err)
			if filesMap[id] != "" && ext == ".dat" {
				continue
			} else {
				filesMap[id] = filepath.Join(absdir, fullname)
			}
		}
	}
	if bc.active == nil {
		active, err := os.OpenFile(filepath.Join(absdir, activeFile), os.O_APPEND|os.O_RDWR|os.O_CREATE, 0600)
		check(err)
		bc.active = active
		bc.writer = bufio.NewWriterSize(bc.active, 200*1024*1024)
	}

	// fid -1 is for active.dat
	bc.keydir, err = loadFromData(-1, bc.active)
	check(err)

	ids := make([]int, 0)
	for i := range filesMap {
		ids = append(ids, i)
	}
	sort.Ints(ids)

	for i := len(ids) - 1; i >= 0; i-- {
		id := ids[i]
		path := filesMap[id]
		df, err := os.OpenFile(path, os.O_RDONLY, 0600)
		check(err)
		bc.buckets[id] = df

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
	if e.fid == -1 {
		f = bc.active
	} else {
		f = bc.buckets[e.fid]
	}
	return bc.keydir.get(f, key)
}

// Put set the value
func (bc *Bitcask) Put(key string, value []byte) error {
	bc.rwlock.Lock()
	defer bc.rwlock.Unlock()
	return bc.keydir.put(-1, bc.active, bc.writer, key, value)
}

// Close for close bc
func (bc *Bitcask) Close() {
	bc.rwlock.Lock()
	defer bc.rwlock.Unlock()

	bc.writer.Flush()
	bc.active.Close()
	for _, f := range bc.buckets {
		if f != nil {
			f.Close()
		}
	}
}
