package cachemachine

import (
	"fmt"
	"github.com/coocood/freecache"
	"gopkg.in/stash.v1"
	"io/ioutil"
	"log"
	"runtime/debug"
	"time"
)

type Logger interface {
	Log(v ...interface{})
	Logf(format string, v ...interface{})
}

type DefaultLogger struct{}

func (l DefaultLogger) Log(v ...interface{}) {
	log.Print(v...)
}

func (l DefaultLogger) Logf(format string, v ...interface{}) {
	log.Printf(format, v...)
}

type CacheSyncTable struct {
	DiskSynced bool
	S3Sync     bool
}

type CacheMachine struct {
	MaxItemSizeInBytes   int
	CacheSyncTable       map[string]CacheSyncTable
	RamCache             *freecache.Cache
	RamCacheSizeInBytes  int
	DiskCache            *stash.Cache
	DiskCacheSizeInBytes int64
	DiskCachePath        string
	DiskCacheSyncTicker  *time.Ticker
	DiskCacheSyncQuit    chan int
	Logger               Logger
}

const (
	DiskCacheSyncInterval = time.Second * 30
)

func NewCacheMachine(maxRamCacheSizeInBytes int, maxItemSizeInBytes int) (cm *CacheMachine, err error) {
	if maxRamCacheSizeInBytes <= 0 {
		err = fmt.Errorf("maxRamCacheSizeInBytes must be greater than 0")
		return nil, err
	}

	if maxItemSizeInBytes <= 0 {
		err = fmt.Errorf("maxItemSizeInBytes must be greater than 0")
		return nil, err
	}

	ramCache := freecache.NewCache(maxRamCacheSizeInBytes)
	if maxRamCacheSizeInBytes > 1024*1024*100 {
		debug.SetGCPercent(20)
	}

	defaultLogger := DefaultLogger{}

	cm = &CacheMachine{
		CacheSyncTable:      make(map[string]CacheSyncTable),
		MaxItemSizeInBytes:  maxRamCacheSizeInBytes,
		RamCache:            ramCache,
		RamCacheSizeInBytes: maxRamCacheSizeInBytes,
		Logger:              defaultLogger,
	}
	return cm, nil
}

func (c *CacheMachine) EnableDiskCache(maxDiskCacheSizeInBytes int64, cachePath string) (err error) {

	if maxDiskCacheSizeInBytes <= 0 {
		err = fmt.Errorf("maxDiskCacheSizeInBytes must be greater than 0")
		return err
	}

	if cachePath == "" {
		err = fmt.Errorf("cachePath must be set")
		return err
	}

	c.DiskCache, err = stash.New(cachePath, maxDiskCacheSizeInBytes, 1024)
	if err != nil {
		return fmt.Errorf("error creating disk cache: %s", err)
	}
	c.DiskCacheSizeInBytes = maxDiskCacheSizeInBytes
	c.DiskCachePath = cachePath

	c.DiskCacheSyncTicker = time.NewTicker(DiskCacheSyncInterval)
	c.DiskCacheSyncQuit = make(chan int)

	go func() {
		for {
			select {
			case <-c.DiskCacheSyncTicker.C:
				c.SyncRamCacheToDiskCache()
			case <-c.DiskCacheSyncQuit:
				c.DiskCacheSyncTicker.Stop()
				return
			}
		}
	}()

	return nil
}

func (c *CacheMachine) DisableDiskCache() {
	c.DiskCacheSyncQuit <- 1
	c.DiskCacheSyncTicker.Stop()
	c.DiskCache = nil
}

func (c *CacheMachine) SyncRamCacheToDiskCache() {
	if c.DiskCache == nil {
		c.Logger.Log("[cachemachine] Disk Cache is not enabled")
		return
	}
	var syncCount int
	for key := range c.CacheSyncTable {
		// TODO: There should only be one thread handling this key at a
		//       time, but just in case, we'll lock the key.
		cacheSync := c.CacheSyncTable[key]
		if !cacheSync.DiskSynced {
			value, err := c.RamCache.Get([]byte(key))
			if err != nil {
				delete(c.CacheSyncTable, key)
				continue
			}
			err = c.DiskCache.Put(key, value)
			if err != nil {
				c.Logger.Log("[cachemachine] Error syncing to disk: ", err)
				continue
			}
			cacheSync.DiskSynced = true
			c.CacheSyncTable[key] = cacheSync
			syncCount++
		}
	}
	if syncCount > 0 {
		c.Logger.Logf("[cachemachine] Synced %d items to disk", syncCount)
	}
}

func (c *CacheMachine) EnableS3Cache(maxItemSizeInBytes int, s3Bucket string) (err error) {
	return fmt.Errorf("not implemented")
}

func (c *CacheMachine) SetLogger(logger *Logger) {
	c.Logger = *logger
}

// Get returns the value for the given key. If the key exists, Get returns
// the value and true. If the key does not exist, Get returns nil and false.
func (c *CacheMachine) Get(key string) (value []byte, ok bool) {
	var err error

	value, err = c.RamCache.Get([]byte(key))
	if err == nil {
		return value, true
	}

	if c.CacheSyncTable[key].DiskSynced {
		valueFromDisk, err := c.DiskCache.Get(key)
		if err == nil {
			value, err := ioutil.ReadAll(valueFromDisk)
			if err == nil {
				return value, true
			}
		}
	}

	return nil, false
}

// Set sets the value for the given key. If the key is larger than 65535 or
// value is larger than 1/1024 of the cache size, the entry will not be
// written to the cache.
func (c *CacheMachine) Set(key string, val []byte) error {
	c.CacheSyncTable[key] = CacheSyncTable{
		DiskSynced: false,
		S3Sync:     false,
	}
	err := c.RamCache.Set([]byte(key), val, 0)
	if err != nil {
		return fmt.Errorf("error setting key %s: %s", key, err)
	}
	return nil
}

// Delete deletes the value for the given key. If the key exists, Delete
// returns true. If the key does not exist, Delete returns false.
func (c *CacheMachine) Delete(key string) bool {
	return c.RamCache.Del([]byte(key))
}

// ClearRamCache clears the cache.
func (c *CacheMachine) ClearRamCache() {
	c.RamCache.Clear()
}

// RamCacheSize returns the size of the cache in bytes.
func (c *CacheMachine) RamCacheSize() int {
	return c.RamCacheSizeInBytes
}

// ClearDiskCache clears the cache.
func (c *CacheMachine) ClearDiskCache() {
	for _, v := range c.DiskCache.Keys() {
		c.DiskCache.Put(v, []byte(""))
	}
}
