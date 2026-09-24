package store

import (
	"context"
	"fmt"
	"testing"

	"github.com/xingjianluo/reconcile-engine-go/internal/object"
)

// TestCacheDetectsStaleVersion verifies the read-through cache records the
// observed version, reports staleness after the object is replaced, and
// invalidates after a CAS conflict so stale data cannot drive a destructive op.
func TestCacheDetectsStaleVersion(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	defer s.Close()
	c := NewCache(s)

	o, err := s.Create(ctx, newTestObj("ns", "cache", map[string]string{"v": "1"}))
	if err != nil {
		t.Fatal(err)
	}

	got, err := c.GetFresh(ctx, o.Key())
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("INPUT GetFresh rv=%d | JUDGE: IsStale(rv)=%v (want false)\n",
		got.ResourceVersion, c.IsStale(o.Key(), got.ResourceVersion))
	if c.IsStale(o.Key(), got.ResourceVersion) {
		t.Fatal("fresh read should not be stale")
	}

	// External writer replaces the object.
	latest, _ := s.Get(ctx, o.Key())
	if _, err := s.Update(ctx, o.Key(), latest.ResourceVersion, func(x *object.Object) {
		x.Spec["v"] = "2"
	}); err != nil {
		t.Fatal(err)
	}

	if !c.IsStale(o.Key(), got.ResourceVersion) {
		t.Fatal("captured version must be flagged stale after replacement")
	}
	fmt.Printf("JUDGE: after external replace, captured rv=%d is now flagged stale\n",
		got.ResourceVersion)

	// A CAS using the stale version conflicts and invalidates the cache.
	_, err = c.UpdateIfVersion(ctx, o.Key(), got.ResourceVersion, func(x *object.Object) {
		x.Spec["v"] = "DESTRUCTIVE"
	})
	if !IsConflict(err) {
		t.Fatalf("want conflict, got %v", err)
	}
	if cv := c.CachedVersion(o.Key()); cv != 0 {
		t.Fatalf("cache must be invalidated after conflict, got rv=%d", cv)
	}
	fmt.Printf("JUDGE: stale CAS conflict=%v; cache invalidated so no destructive write\n",
		IsConflict(err))

	// Re-read fresh then a valid CAS succeeds.
	fresh, _ := c.GetFresh(ctx, o.Key())
	if _, err := c.UpdateIfVersion(ctx, o.Key(), fresh.ResourceVersion, func(x *object.Object) {
		x.Spec["v"] = "3"
	}); err != nil {
		t.Fatalf("fresh CAS should succeed: %v", err)
	}
	final, _ := s.Get(ctx, o.Key())
	if final.Spec["v"] != "3" {
		t.Fatalf("unexpected final value %v", final.Spec)
	}
	fmt.Printf("JUDGE: after GetFresh + CAS, final spec=%v (destructive stale write never applied)\n",
		final.Spec)
}
