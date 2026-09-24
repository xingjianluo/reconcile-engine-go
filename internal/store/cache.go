package store

import (
	"context"
	"sync"

	"github.com/xingjianluo/reconcile-engine-go/internal/object"
)

// Cache is a read-through cache in front of a Store. It remembers the version
// it last observed so a reconcile loop can detect that the object it holds
// has been replaced underneath it. All writes go straight through with CAS;
// a successful write updates the cache, while a conflict invalidates it so
// stale data can never be used for a subsequent destructive operation.
type Cache struct {
	store Store

	mu   sync.RWMutex
	live map[string]*object.Object
}

// NewCache wraps a Store with an invalidating read cache.
func NewCache(s Store) *Cache {
	return &Cache{store: s, live: make(map[string]*object.Object)}
}

// GetFresh always reads through to the store and records the result. It is
// the safe entry point before any destructive operation: the returned
// version is the one the caller must present to Update/Delete/Remove.
func (c *Cache) GetFresh(ctx context.Context, key string) (*object.Object, error) {
	obj, err := c.store.Get(ctx, key)
	if err != nil {
		// A missing object means our cache (if any) is stale; drop it.
		c.Invalidate(key)
		return nil, err
	}
	c.mu.Lock()
	c.live[key] = obj.Clone()
	c.mu.Unlock()
	return obj, nil
}

// CachedVersion returns the version currently held in cache, or 0 if absent.
func (c *Cache) CachedVersion(key string) int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if o := c.live[key]; o != nil {
		return o.ResourceVersion
	}
	return 0
}

// IsStale reports whether the supplied version differs from the version
// currently live in the backing store (i.e. the object was replaced after the
// caller read it). It compares against the source of truth rather than only
// the locally cached copy, so writes performed by other actors are detected.
func (c *Cache) IsStale(key string, version int64) bool {
	cur, err := c.store.Get(context.Background(), key)
	if err != nil {
		// Gone or unreadable: a captured version cannot be trusted for a
		// destructive operation, so treat it as stale.
		return true
	}
	c.mu.Lock()
	c.live[key] = cur.Clone()
	c.mu.Unlock()
	return cur.ResourceVersion != version
}

// UpdateIfVersion CAS-modifies an object through the store. On success the
// cache is advanced to the new object; on conflict the cached entry is
// invalidated so the next read is forced to refresh.
func (c *Cache) UpdateIfVersion(ctx context.Context, key string, expectedVersion int64, fn func(*object.Object)) (*object.Object, error) {
	out, err := c.store.Update(ctx, key, expectedVersion, fn)
	if err != nil {
		if IsConflict(err) {
			c.Invalidate(key)
		}
		return nil, err
	}
	c.mu.Lock()
	c.live[key] = out.Clone()
	c.mu.Unlock()
	return out, nil
}

// Invalidate drops any cached copy of a key.
func (c *Cache) Invalidate(key string) {
	c.mu.Lock()
	delete(c.live, key)
	c.mu.Unlock()
}

// IsConflict reports whether err is an optimistic-concurrency failure.
func IsConflict(err error) bool {
	if err == nil {
		return false
	}
	_, ok := err.(*ErrConflict)
	return ok
}
