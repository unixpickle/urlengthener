package main

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"
)

const seekBufferSize = 4096

// A KVStore is a key-value store with incrementing
// integer keys and raw byte values.
type KVStore struct {
	file *os.File

	// TODO: use a read-lock and multiple *os.File instances
	// to do multiple searches at once.
	lock sync.Mutex
}

// NewKVStore creates a KVStore with the given file.
// If the storage file does not exist, it is created.
func NewKVStore(path string) (*KVStore, error) {
	var f *os.File
	if _, err := os.Stat(path); os.IsNotExist(err) {
		f, err = os.Create(path)
		if err != nil {
			return nil, err
		}
	} else {
		f, err = os.Open(path)
		if err != nil {
			return nil, err
		}
	}
	return &KVStore{file: f}, nil
}

// Insert inserts a value into the KVStore, allocating a
// new key in the process.
func (k *KVStore) Insert(value []byte) (key int64, err error) {
	k.lock.Lock()
	defer k.lock.Unlock()

	off, err := k.file.Seek(0, os.SEEK_END)
	if err != nil {
		return
	}
	if off > 0 {
		// Subtract 1 from off to discount the trailing newline.
		lastNewline, err := k.newlineBefore(off - 1)
		if err != nil {
			return 0, err
		}
		key, err = k.readKey(lastNewline + 1)
		if err != nil {
			return 0, err
		}
		key++
	}
	k.file.Seek(0, os.SEEK_END)
	entryStr := fmt.Sprintf("%d %s\n", key, base64.StdEncoding.EncodeToString(value))
	if _, err := k.file.Write([]byte(entryStr)); err != nil {
		k.file.Truncate(off)
		return 0, err
	}
	return
}

// Get returns the value for the given key, or nil if no
// entry was found with the given key.
func (k *KVStore) Get(key int64) ([]byte, error) {
	k.lock.Lock()
	k.lock.Unlock()

	max, err := k.file.Seek(0, os.SEEK_END)
	if err != nil {
		return nil, err
	}
	if max == 0 {
		return nil, nil
	}
	max--
	min := int64(0)

	for max > min {
		idx := (max + min) / 2
		newlineIdx, err := k.newlineBefore(idx)
		if err != nil {
			return nil, err
		}
		thisKey, err := k.readKey(newlineIdx + 1)
		if err != nil {
			return nil, err
		}
		if thisKey == key {
			val, err := k.readValue()
			if err != nil {
				return nil, err
			}
			return base64.StdEncoding.DecodeString(string(val))
		} else if thisKey < key {
			k.readValue()
			min, err = k.file.Seek(0, os.SEEK_CUR)
			if err != nil {
				return nil, err
			}
			// Want min to point to the first byte of the next entry,
			// rather than the newline right before the next entry,
			// ensuring that we exclude the current entry from later
			// search iterations.
			min++
		} else if thisKey > key {
			// This is a non-inclusive upper bound, so the current
			// entry will never be considered again.
			max = newlineIdx
		}
	}
	return nil, nil
}

// newlineBefore finds the index of the first newline
// before the given index in the file.
// If none exists, -1 is returned.
func (k *KVStore) newlineBefore(idx int64) (int64, error) {
	for idx > 0 {
		bufSize := int64(seekBufferSize)
		if bufSize > idx {
			bufSize = idx
		}
		idx -= bufSize
		k.file.Seek(idx, os.SEEK_SET)
		buf := make([]byte, bufSize)
		n, err := io.ReadFull(k.file, buf)
		if err != nil && err != io.EOF {
			return 0, err
		}
		for i := n - 1; i >= 0; i-- {
			if buf[i] == '\n' {
				return idx + int64(i), nil
			}
		}
	}
	return -1, nil
}

// readKey reads the key at the given offset in the file.
func (k *KVStore) readKey(idx int64) (int64, error) {
	k.file.Seek(idx, os.SEEK_SET)
	var b bytes.Buffer
	for {
		next := make([]byte, 1)
		if _, err := io.ReadFull(k.file, next); err != nil {
			return 0, err
		}
		if next[0] == ' ' {
			break
		}
		b.Write(next)
	}
	return strconv.ParseInt(b.String(), 10, 64)
}

// readValue reads the data at the current file offset,
// stopping when it hits a newline.
// Upon success, the file will be seeked to the newline.
// This is meant to be used directly after readKey().
func (k *KVStore) readValue() ([]byte, error) {
	var b bytes.Buffer
	for {
		next := make([]byte, 1)
		if _, err := io.ReadFull(k.file, next); err != nil {
			return nil, err
		}
		if next[0] == '\n' {
			break
		}
		b.Write(next)
	}
	return b.Bytes(), nil
}
