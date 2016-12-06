package main

import (
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestKVStoreBasic(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "kvstore")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	store, err := NewKVStore(filepath.Join(tempDir, "store1"))
	if err != nil {
		t.Fatal(err)
	}
	if data, err := store.Get(1); err != nil {
		t.Error(err)
	} else if data != nil {
		t.Error("unexpected result", data)
	}

	if key, err := store.Insert([]byte("hello world!")); err != nil {
		t.Fatal(err)
	} else if key != 0 {
		t.Fatal("unexpected key:", key)
	}
	val, err := store.Get(0)
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "hello world!" {
		t.Error("unexpected value:", string(val))
	}
	if data, err := store.Get(1); err != nil {
		t.Error(err)
	} else if data != nil {
		t.Error("unexpected result:", data)
	}

	if key, err := store.Insert([]byte("goodbye world!")); err != nil {
		t.Fatal(err)
	} else if key != 1 {
		t.Fatal("unexpected key:", key)
	}
	val, err = store.Get(0)
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "hello world!" {
		t.Error("unexpected value:", string(val))
	}
	val, err = store.Get(1)
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "goodbye world!" {
		t.Error("unexpected value:", string(val))
	}
	if val, err := store.Get(2); err != nil {
		t.Fatal(err)
	} else if val != nil {
		t.Error("unexpected result:", val)
	}

	store.Close()
	store, err = NewKVStore(filepath.Join(tempDir, "store1"))
	if err != nil {
		t.Fatal(err)
	}

	if key, err := store.Insert([]byte("static world")); err != nil {
		t.Fatal(err)
	} else if key != 2 {
		t.Fatal("unexpected key:", key)
	}
	val, err = store.Get(0)
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "hello world!" {
		t.Error("unexpected value:", string(val))
	}
	val, err = store.Get(1)
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "goodbye world!" {
		t.Error("unexpected value:", string(val))
	}
	val, err = store.Get(2)
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "static world" {
		t.Error("unexpected value:", string(val))
	}
	if val, err := store.Get(3); err != nil {
		t.Fatal(err)
	} else if val != nil {
		t.Error("unexpected result:", val)
	}
}

func TestKVStoreRandom(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "kvstore")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	store, err := NewKVStore(filepath.Join(tempDir, "store1"))
	if err != nil {
		t.Fatal(err)
	}

	mapping := map[int64][]byte{}

	for i := 0; i < 1000; i++ {
		value := []byte{}
		for j := 0; j < rand.Intn(10); j++ {
			value = append(value, byte(rand.Intn(256)))
		}
		key, err := store.Insert(value)
		if err != nil {
			t.Fatal(err)
		}
		mapping[key] = value
		if key != int64(i) {
			t.Fatalf("expected key %d but got %d", i, key)
		}

		randKey := int64(rand.Intn(100))
		expected := mapping[randKey]
		actual, err := store.Get(randKey)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(actual, expected) {
			t.Fatalf("iter %d: key %d should give %v but got %v", i, randKey,
				expected, actual)
		}
	}
}
