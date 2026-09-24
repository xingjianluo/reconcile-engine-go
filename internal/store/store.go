// Package store provides an in-memory, versioned, concurrency-safe object
// store that simulates a declarative API server with optimistic concurrency.
package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/xingjianluo/reconcile-engine-go/internal/object"
)

// EventType enumerates store change kinds.
type EventType int

const (
	EventUpsert EventType = iota
	EventDelete
)

// Event is a single observed change to an object.
type Event struct {
	Type EventType
	Key  string
	// Object is the post-change object; nil for EventDelete.
	Object *object.Object
}

// Store is the versioned object store contract.
//
// All mutating calls are compare-and-swap: callers must present the
// ResourceVersion they read. If the object has since been replaced, the call
// fails with a conflict so the caller can re-read and retry instead of
// applying a destructive write against stale data.
type Store interface {
	// Create adds a new object. It conflicts if the key already exists.
	Create(ctx context.Context, obj *object.Object) (*object.Object, error)
	// Get returns the current object for a key.
	Get(ctx context.Context, key string) (*object.Object, error)
	// List returns all current objects.
	List(ctx context.Context) ([]*object.Object, error)
	// Update applies fn to the object identified by expectedVersion.
	// It returns ErrConflict if the live version differs from expectedVersion.
	Update(ctx context.Context, key string, expectedVersion int64, fn func(*object.Object)) (*object.Object, error)
	// Delete marks the object as deleting (sets a deletion timestamp) when the
	// object still has finalizers, or removes it outright when none remain.
	// It returns ErrConflict on a stale expectedVersion.
	Delete(ctx context.Context, key string, expectedVersion int64) (*object.Object, error)
	// Remove irrevocably deletes the object; only valid once it has no
	// finalizers and already carries a deletion timestamp.
	Remove(ctx context.Context, key string, expectedVersion int64) error
	// Watch streams change events from the moment Watch is called.
	Watch(ctx context.Context) (<-chan Event, error)
}

// ErrConflict indicates an optimistic-concurrency failure: the object was
// replaced between read and write. Callers must re-read and retry. It is
// returned as a pointer so callers can use errors.As to distinguish it.
type ErrConflict struct {
	Key             string
	ExpectedVersion int64
	ActualVersion   int64
}

func (e *ErrConflict) Error() string {
	return fmt.Sprintf("store: conflict on %q: expected resourceVersion %d, found %d",
		e.Key, e.ExpectedVersion, e.ActualVersion)
}

// ErrAlreadyExists is returned by Create when the key is already present.
var ErrAlreadyExists = errors.New("object already exists")

// ErrFinalizersRemain is returned by Remove when finalizers still block
// physical deletion.
var ErrFinalizersRemain = errors.New("object cannot be removed: finalizers remain")

// ErrNotDeleting is returned by Remove when no deletion timestamp is set.
var ErrNotDeleting = errors.New("object cannot be removed: no deletion timestamp set")

// ErrStoreClosed is returned when a closed store is used after Close.
var ErrStoreClosed = errors.New("store: closed")
