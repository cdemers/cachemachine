package cachemachine

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
)

func TestNewCacheMachine(t *testing.T) {
	cacheMachine, err := NewCacheMachine(10, 1024)
	if err != nil {
		t.Errorf("Error creating cache machine: %s", err)
	}
	if cacheMachine == nil {
		t.Errorf("Expected cache to be initialized, got nil")
	}

	cacheMachine, err = NewCacheMachine(0, 1024)
	if err == nil {
		t.Errorf("Expected error creating cache machine with 0 capacity")
	}
	if cacheMachine != nil {
		t.Errorf("Expected cache to be nil, got %v", cacheMachine)
	}

	cacheMachine, err = NewCacheMachine(10, 0)
	if err == nil {
		t.Errorf("Expected error creating cache machine with 0 as a maximum cached item size")
	}
	if cacheMachine != nil {
		t.Errorf("Expected cache to be nil, got %v", cacheMachine)
	}
}

func TestCacheMachine_Size(t *testing.T) {
	CacheMachine, err := NewCacheMachine(10, 1024)
	if err != nil {
		t.Errorf("Error creating cache machine: %s", err)
	}
	if CacheMachine.RamCacheSize() != 10 {
		t.Errorf("Expected cache size to be 10, got %d", CacheMachine.RamCacheSize())
	}
}

func TestCacheMachine_Set(t *testing.T) {
	CacheMachine, err := NewCacheMachine(10, 1024)
	if err != nil {
		t.Errorf("Error creating cache machine: %s", err)
	}
	err = CacheMachine.Set("key1", []byte("12345"))
	if err != nil {
		t.Errorf("Expected no error setting key1, got %s", err)
	}
	err = CacheMachine.Set("key2", []byte("67890"))
	if err != nil {
		t.Errorf("Expected no error setting key2, got %s", err)
	}
	err = CacheMachine.Set("keyX", []byte("abcde"))
	if err != nil {
		t.Errorf("Expected no error setting keyX, got %s", err)
	}
}

func TestCacheMachine_Get(t *testing.T) {
	CacheMachine, err := NewCacheMachine(10, 1024)
	if err != nil {
		t.Errorf("Error creating cache machine: %s", err)
	}

	err = CacheMachine.Set("key1", []byte("12345"))
	if err != nil {
		t.Errorf("Expected no error setting key1, got %s", err)
	}

	err = CacheMachine.Set("key2", []byte("67890"))
	if err != nil {
		t.Errorf("Expected no error setting key2, got %s", err)
	}

	value, ok := CacheMachine.Get("key1")
	if !ok {
		t.Errorf("Expected no cache miss getting key1, got %v", ok)
	}
	if string(value) != "12345" {
		t.Errorf("Expected value to be 12345, got %s", value)
	}

	value, ok = CacheMachine.Get("key2")
	if !ok {
		t.Errorf("Expected no cache miss getting key2, got %v", ok)
	}
	if string(value) != "67890" {
		t.Errorf("Expected value to be 67890, got %s", value)
	}
}

func TestCacheMachine_EnableDiskCache(t *testing.T) {
	CacheMachine, err := NewCacheMachine(10, 1024)
	if err != nil {
		t.Errorf("Error creating cache machine: %s", err)
	}

	tmpFolder, err := createTempFolder()
	if err != nil {
		t.Errorf("Error creating temp folder: %s", err)
	}
	defer removeTempFolder(tmpFolder)

	err = CacheMachine.EnableDiskCache(10, tmpFolder)
	if err != nil {
		t.Errorf("Expected no error enabling disk cache, got %s", err)
	}

	err = CacheMachine.Set("key1", []byte("12345"))
	if err != nil {
		t.Errorf("Expected no error setting key1, got %s", err)
	}

	// TODO: Test that the file was written to disk

	CacheMachine.DisableDiskCache()
}

func createTempFolder() (string, error) {
	tmpFolder, err := ioutil.TempDir("", "test")
	if err != nil {
		return "", fmt.Errorf("error creating temp folder: %s", err)
	}
	return tmpFolder, nil
}

func removeTempFolder(tmpFolder string) {
	err := os.RemoveAll(tmpFolder)
	if err != nil {
		fmt.Printf("error removing temp folder: %s", err)
	}
}
