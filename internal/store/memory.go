package store

import (
	"context"
	"sync"

	"github.com/xingjianluo/reconcile-engine-go/internal/object"
)

// watchBuffer bounds per-subscriber backlog. A subscriber that cannot keep up
// is closed rather than allowing memory to grow without limit.
const watchBuffer = 64

// MemoryStore is the in-memory Store implementation. Every mutation is
// serialized by a single mutex and produces a new monotonically increasing
// resource version, which gives the whole store a total ordering of writes
// and makes optimistic concurrency control straightforward.
type MemoryStore struct {
	mu        sync.RWMutex
	data      map[string]*object.Object
	nextRV    int64
	watchers  map[uint64]chan Event
	watcherID uint64
	closed    bool
}

// NewMemoryStore constructs an empty MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		data:     make(map[string]*object.Object),
		nextRV:   1,
		watchers: make(map[uint64]chan Event),
	}
}

// Create adds a new object. It conflicts (ErrAlreadyExists) if the key exists.
// The supplied ResourceVersion is ignored; the store assigns the next one.
func (s *MemoryStore) Create(ctx context.Context, obj *object.Object) (*object.Object, error) {
	if obj == nil {
		return nil, object.ErrNotFound
	}
	key := obj.Key()

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, ErrStoreClosed
	}
	if _, ok := s.data[key]; ok {
		return nil, ErrAlreadyExists
	}

	stored := obj.Clone()
	stored.ResourceVersion = s.nextRV
	s.nextRV++
	s.data[key] = stored

	s.publishLocked(Event{Type: EventUpsert, Key: key, Object: stored.Clone()})
	return stored.Clone(), nil
}

// Get returns a defensive copy of the current object for a key.
func (s *MemoryStore) Get(ctx context.Context, key string) (*object.Object, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	obj, ok := s.data[key]
	if !ok {
		return nil, object.ErrNotFound
	}
	return obj.Clone(), nil
}

// List returns defensive copies of all current objects.
func (s *MemoryStore) List(ctx context.Context) ([]*object.Object, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*object.Object, 0, len(s.data))
	for _, obj := range s.data {
		out = append(out, obj.Clone())
	}
	return out, nil
}

// Update applies fn to the object identified by expectedVersion. It returns
// *ErrConflict if the live version differs from expectedVersion, so callers
// cannot mutate stale data. fn mutates a scratch copy and its changes are
// persisted only when the version still matches.
func (s *MemoryStore) Update(ctx context.Context, key string, expectedVersion int64, fn func(*object.Object)) (*object.Object, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, ErrStoreClosed
	}
	cur, ok := s.data[key]
	if !ok {
		return nil, object.ErrNotFound
	}
	if cur.ResourceVersion != expectedVersion {
		return nil, &ErrConflict{Key: key, ExpectedVersion: expectedVersion, ActualVersion: cur.ResourceVersion}
	}

	updated := cur.Clone()
	fn(updated)
	// Identity and version are never user-editable through Update.
	updated.Namespace = cur.Namespace
	updated.Name = cur.Name
	updated.ResourceVersion = s.nextRV
	s.nextRV++
	s.data[key] = updated

	s.publishLocked(Event{Type: EventUpsert, Key: key, Object: updated.Clone()})
	return updated.Clone(), nil
}

// Delete performs the first phase of finalizer deletion. When the object has
// no finalizers it is removed outright; otherwise its deletion timestamp is
// set (idempotently if already set) so finalizer holders can run cleanup.
// A stale expectedVersion yields *ErrConflict.
func (s *MemoryStore) Delete(ctx context.Context, key string, expectedVersion int64) (*object.Object, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, ErrStoreClosed
	}
	cur, ok := s.data[key]
	if !ok {
		return nil, object.ErrNotFound
	}
	if cur.ResourceVersion != expectedVersion {
		return nil, &ErrConflict{Key: key, ExpectedVersion: expectedVersion, ActualVersion: cur.ResourceVersion}
	}

	// No finalizers -> physical removal immediately.
	if len(cur.Finalizers) == 0 {
		delete(s.data, key)
		s.publishLocked(Event{Type: EventDelete, Key: key, Object: nil})
		return nil, nil
	}

	// Finalizers remain -> advance to "deleting" (idempotent).
	if cur.DeletionTimestamp == 0 {
		updated := cur.Clone()
		updated.DeletionTimestamp = object.DeletionTimestampNow()
		updated.ResourceVersion = s.nextRV
		s.nextRV++
		s.data[key] = updated
		s.publishLocked(Event{Type: EventUpsert, Key: key, Object: updated.Clone()})
		return updated.Clone(), nil
	}
	return cur.Clone(), nil
}

// Remove irrevocably deletes an object. It is valid only once the object
// carries a deletion timestamp and has no finalizers. The expectedVersion
// must still match so a caller operating on replaced/stale data cannot
// destroy a different object.
func (s *MemoryStore) Remove(ctx context.Context, key string, expectedVersion int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrStoreClosed
	}
	cur, ok := s.data[key]
	if !ok {
		return object.ErrNotFound
	}
	if cur.ResourceVersion != expectedVersion {
		return &ErrConflict{Key: key, ExpectedVersion: expectedVersion, ActualVersion: cur.ResourceVersion}
	}
	if cur.DeletionTimestamp == 0 {
		return ErrNotDeleting
	}
	if len(cur.Finalizers) != 0 {
		return ErrFinalizersRemain
	}
	delete(s.data, key)
	s.publishLocked(Event{Type: EventDelete, Key: key, Object: nil})
	return nil
}

// Watch streams change events from the moment it is called. Cancelling ctx
// removes the subscription. A slow consumer is disconnected (its channel
// closed) to keep bounded memory; callers treat close as "resync needed".
func (s *MemoryStore) Watch(ctx context.Context) (<-chan Event, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, ErrStoreClosed
	}
	id := s.watcherID
	s.watcherID++
	ch := make(chan Event, watchBuffer)
	s.watchers[id] = ch
	s.mu.Unlock()

	go func() {
		<-ctx.Done()
		s.mu.Lock()
		if w, ok := s.watchers[id]; ok {
			delete(s.watchers, id)
			close(w)
		}
		s.mu.Unlock()
	}()
	return ch, nil
}

// publishLocked dispatches an event to all watchers. Caller holds mu.
func (s *MemoryStore) publishLocked(ev Event) {
	for id, ch := range s.watchers {
		select {
		case ch <- ev:
		default:
			// Slow consumer: disconnect it so the store cannot be blocked or
			// grow unbounded buffers.
			delete(s.watchers, id)
			close(ch)
		}
	}
}

// Close releases watcher subscriptions. Mainly useful for deterministic
// test teardown.
func (s *MemoryStore) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	for id, ch := range s.watchers {
		delete(s.watchers, id)
		close(ch)
	}
}
