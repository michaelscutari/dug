package db

import (
	"container/list"
	"database/sql"
	"sync"
)

const dirCacheSize = 4096

type dirCacheEntry struct {
	key   string
	value int64
}

type dirCache struct {
	mu    sync.Mutex
	max   int
	ll    *list.List
	items map[string]*list.Element
}

func newDirCache(max int) *dirCache {
	return &dirCache{
		max:   max,
		ll:    list.New(),
		items: make(map[string]*list.Element),
	}
}

func (c *dirCache) Get(key string) (int64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if el, ok := c.items[key]; ok {
		c.ll.MoveToFront(el)
		return el.Value.(dirCacheEntry).value, true
	}
	return 0, false
}

func (c *dirCache) Set(key string, value int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if el, ok := c.items[key]; ok {
		el.Value = dirCacheEntry{key: key, value: value}
		c.ll.MoveToFront(el)
		return
	}

	el := c.ll.PushFront(dirCacheEntry{key: key, value: value})
	c.items[key] = el

	if c.ll.Len() > c.max {
		last := c.ll.Back()
		if last == nil {
			return
		}
		c.ll.Remove(last)
		delete(c.items, last.Value.(dirCacheEntry).key)
	}
}

var dbDirCaches sync.Map // map[*sql.DB]*dirCache

func getDirCache(db *sql.DB) *dirCache {
	if db == nil {
		return nil
	}
	if existing, ok := dbDirCaches.Load(db); ok {
		return existing.(*dirCache)
	}
	cache := newDirCache(dirCacheSize)
	actual, _ := dbDirCaches.LoadOrStore(db, cache)
	return actual.(*dirCache)
}
