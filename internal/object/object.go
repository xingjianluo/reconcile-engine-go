// Package object defines the versioned resource object that flows through
// the reconciliation engine, together with finalizer and tombstone helpers.
package object

import (
	"errors"
	"fmt"
	"time"
)

// DeletionTimestamp is set (non-zero) when a resource has been requested for
// deletion. While it is set and finalizers remain, the object MUST continue
// to exist and be reconciled in "deleting" state; it only disappears once
// every finalizer has been removed.
type DeletionTimestamp int64

// DependencyRef names another object that the owning object depends on. A
// dependent is allowed to reconcile only after every referenced dependency
// exists AND reports ready. A namespace-less reference resolves inside the
// owning object's namespace (the engine expands it at graph-build time).
type DependencyRef struct {
	Namespace string
	Name      string
}

// Key renders the namespaced identity of the referenced object.
func (r DependencyRef) Key() string {
	return MakeKey(r.Namespace, r.Name)
}

// Object is a versioned resource. ResourceVersion is assigned and incremented
// by the store on every successful write and is used for optimistic
// concurrency control: callers present the version they read and a write
// against a replaced object fails with a conflict.
type Object struct {
	Namespace string
	Name      string

	// ResourceVersion is 0 for objects that have not yet been persisted.
	ResourceVersion int64

	// Spec is the user-desired payload (opaque to the engine).
	Spec map[string]string
	// Status is the observed/actual payload written by reconcilers.
	Status map[string]string

	// Dependencies are the other objects that must exist and be Ready before
	// this object is allowed to reconcile while alive. The slice is part of
	// desired state and declared by the producer of the object.
	Dependencies []DependencyRef

	// Ready is the reconciler-reported readiness signal. A dependency only
	// unblocks its dependents once Ready is true; an object being deleted is
	// never considered ready even if this flag stays set.
	Ready bool

	// Finalizers block physical deletion until each is removed.
	Finalizers []string

	// DeletionTimestamp != 0 marks the object as being deleted.
	DeletionTimestamp DeletionTimestamp
}

// Key returns the namespaced identity of the object.
func (o *Object) Key() string {
	return MakeKey(o.Namespace, o.Name)
}

// MakeKey joins namespace and name into a stable identity string.
func MakeKey(namespace, name string) string {
	if namespace == "" {
		return name
	}
	return namespace + "/" + name
}

// BeingDeleted reports whether a deletion has been requested.
func (o *Object) BeingDeleted() bool { return o.DeletionTimestamp != 0 }

// HasFinalizer reports whether f is present.
func (o *Object) HasFinalizer(f string) bool {
	for _, x := range o.Finalizers {
		if x == f {
			return true
		}
	}
	return false
}

// AddFinalizer appends f if absent. It mutates the receiver in place.
func (o *Object) AddFinalizer(f string) {
	if !o.HasFinalizer(f) {
		o.Finalizers = append(o.Finalizers, f)
	}
}

// RemoveFinalizer removes f if present and reports whether anything changed.
// It allocates a fresh slice so the result never aliases the old backing
// array, keeping repeated removals and concurrent cloned reads safe.
func (o *Object) RemoveFinalizer(f string) bool {
	if !o.HasFinalizer(f) {
		return false
	}
	out := make([]string, 0, len(o.Finalizers))
	for _, x := range o.Finalizers {
		if x != f {
			out = append(out, x)
		}
	}
	o.Finalizers = out
	return true
}

// Clone returns a deep copy so concurrent readers cannot observe a mutating
// reconcile's in-progress edits. Spec and Status are always allocated so
// callers can write into the clone without a nil-map panic.
func (o *Object) Clone() *Object {
	if o == nil {
		return nil
	}
	cp := *o
	cp.Spec = make(map[string]string, len(o.Spec))
	for k, v := range o.Spec {
		cp.Spec[k] = v
	}
	cp.Status = make(map[string]string, len(o.Status))
	for k, v := range o.Status {
		cp.Status[k] = v
	}
	if o.Dependencies != nil {
		cp.Dependencies = append([]DependencyRef(nil), o.Dependencies...)
	} else {
		cp.Dependencies = nil
	}
	if o.Finalizers != nil {
		cp.Finalizers = append([]string(nil), o.Finalizers...)
	} else {
		cp.Finalizers = nil
	}
	return &cp
}

// String renders an audit-friendly one-line description.
func (o *Object) String() string {
	return fmt.Sprintf("Object(key=%s rv=%d ready=%t deps=%d deleting=%t finalizers=%v)",
		o.Key(), o.ResourceVersion, o.Ready, len(o.Dependencies), o.BeingDeleted(), o.Finalizers)
}

// ErrNotFound is returned by the store when no object exists for a key.
var ErrNotFound = errors.New("object not found")

// DeletionTimestampNow returns a deletion timestamp for the current wall
// clock, expressed in Unix nanoseconds.
func DeletionTimestampNow() DeletionTimestamp {
	return DeletionTimestamp(time.Now().UnixNano())
}

// AnnotateTouched is a test/demo helper that records a benign touch without
// altering desired spec or status, used to generate a no-op change event.
func (o *Object) AnnotateTouched() {
	if o.Status == nil {
		o.Status = map[string]string{}
	}
}
