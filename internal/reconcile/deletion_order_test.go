package reconcile

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/reconcile-engine-go/internal/object"
	"github.com/xingjianluo/reconcile-engine-go/internal/store"
)

// deleteOrderRecorder subscribes to physical-deletion events (EventDelete) and
// records the exact order in which objects physically disappear from the
// store. Only the recorder goroutine appends, so the observed sequence is
// race-free; assertions read the slice only after the recorder has stopped.
type deleteOrderRecorder struct {
	mu    sync.Mutex
	order []string
	done  chan struct{}
}

// newDeleteOrderRecorder starts draining ev immediately. The store may
// disconnect a slow consumer; draining in a dedicated goroutine keeps that
// from ever happening in these tests.
func newDeleteOrderRecorder(ev <-chan store.Event) *deleteOrderRecorder {
	r := &deleteOrderRecorder{done: make(chan struct{})}
	go func() {
		defer close(r.done)
		for e := range ev {
			if e.Type == store.EventDelete {
				r.mu.Lock()
				r.order = append(r.order, e.Key)
				r.mu.Unlock()
			}
		}
	}()
	return r
}

func (r *deleteOrderRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.order...)
}

// positionOf returns the index of key in the observed deletion order.
func positionOf(order []string, key string) int {
	for i, k := range order {
		if k == key {
			return i
		}
	}
	return -1
}

// waitForGone polls until key is physically absent.
func waitForGone(t *testing.T, ctx context.Context, s store.Store, key string) {
	t.Helper()
	if !waitFor(3*time.Second, func() bool {
		_, err := s.Get(ctx, key)
		return errors.Is(err, object.ErrNotFound)
	}) {
		t.Fatalf("%s was not physically removed", key)
	}
}

// requestDelete marks key for deletion using its latest version. Multiple
// callers may race (the controller itself writes through the same CAS), so a
// stale-version conflict is resolved by re-reading and retrying: the request
// is idempotent, and an already-gone / already-deleting object means the
// desired deletion is already in progress.
func requestDelete(t *testing.T, ctx context.Context, s store.Store, key string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		cur, err := s.Get(ctx, key)
		if err != nil {
			if errors.Is(err, object.ErrNotFound) {
				return
			}
			t.Fatal(err)
		}
		_, err = s.Delete(ctx, key, cur.ResourceVersion)
		if err == nil || errors.Is(err, object.ErrNotFound) {
			return
		}
		if store.IsConflict(err) && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
			continue
		}
		t.Fatalf("delete %s: %v", key, err)
	}
}

// TestDeletionOrderChainPhysical asserts the REAL physical removal order of a
// three-level dependency chain (leaf -> mid -> root) when all three deletion
// requests are issued concurrently (some duplicated) while several workers
// run. The depended-on resource must not disappear before every dependent on
// it has physically disappeared: leaf first, mid second, root last — not
// merely "all gone at the end".
func TestDeletionOrderChainPhysical(t *testing.T) {
	r := newDepRig(t, 4, 0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.ctrl.Run(ctx)
	s := r.store

	createObj(t, ctx, s, "ns", "leaf", object.DependencyRef{Name: "mid"})
	createObj(t, ctx, s, "ns", "mid", object.DependencyRef{Name: "root"})
	createObj(t, ctx, s, "ns", "root")
	for _, k := range []string{"ns/root", "ns/mid", "ns/leaf"} {
		if !waitFor(5*time.Second, func() bool { return readyKey(t, ctx, s, k) }) {
			t.Fatalf("%s did not converge", k)
		}
	}

	watchCtx, stopWatch := context.WithCancel(ctx)
	defer stopWatch()
	evCh, err := s.Watch(watchCtx)
	if err != nil {
		t.Fatal(err)
	}
	rec := newDeleteOrderRecorder(evCh)

	// Fire all deletion requests concurrently, including duplicate requests,
	// to prove ordering does not depend on request timing or uniqueness.
	requests := []string{"ns/root", "ns/mid", "ns/leaf", "ns/root", "ns/mid"}
	var wg sync.WaitGroup
	for _, k := range requests {
		wg.Add(1)
		go func(key string) {
			defer wg.Done()
			requestDelete(t, ctx, s, key)
		}(k)
	}
	wg.Wait()

	waitForGone(t, ctx, s, "ns/leaf")
	waitForGone(t, ctx, s, "ns/mid")
	waitForGone(t, ctx, s, "ns/root")
	stopWatch()
	<-rec.done

	order := rec.snapshot()
	iLeaf, iMid, iRoot := positionOf(order, "ns/leaf"), positionOf(order, "ns/mid"), positionOf(order, "ns/root")
	if iLeaf < 0 || iMid < 0 || iRoot < 0 {
		t.Fatalf("every member must have produced one physical delete event, got %v", order)
	}
	if !(iLeaf < iMid && iMid < iRoot) {
		t.Fatalf("physical deletion order must be leaf<mid<root, got %v", order)
	}
	// Each key may physically disappear exactly once.
	counts := map[string]int{}
	for _, k := range order {
		counts[k]++
	}
	for _, k := range []string{"ns/leaf", "ns/mid", "ns/root"} {
		if counts[k] != 1 {
			t.Fatalf("%s deleted %d times, want exactly 1 (order=%v)", k, counts[k], order)
		}
	}
	fmt.Printf("INPUT concurrent delete root,mid,leaf (+duplicates), 4 workers | JUDGE: physical delete order=%v\n", order)
}

// TestDeletionOrderFanInPhysical asserts the fan-in case: one shared resource
// is depended on by several independent dependents. When the shared resource
// and every dependent are deleted concurrently, the shared resource must be
// the LAST physical deletion; each dependent must disappear before it.
func TestDeletionOrderFanInPhysical(t *testing.T) {
	r := newDepRig(t, 4, 0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.ctrl.Run(ctx)
	s := r.store

	createObj(t, ctx, s, "ns", "shared")
	const leaves = 5
	leafKeys := make([]string, 0, leaves)
	for i := 0; i < leaves; i++ {
		name := fmt.Sprintf("leaf%02d", i)
		createObj(t, ctx, s, "ns", name, object.DependencyRef{Name: "shared"})
		leafKeys = append(leafKeys, "ns/"+name)
	}
	for _, k := range append(append([]string{}, leafKeys...), "ns/shared") {
		if !waitFor(5*time.Second, func() bool { return readyKey(t, ctx, s, k) }) {
			t.Fatalf("%s did not converge", k)
		}
	}

	watchCtx, stopWatch := context.WithCancel(ctx)
	defer stopWatch()
	evCh, err := s.Watch(watchCtx)
	if err != nil {
		t.Fatal(err)
	}
	rec := newDeleteOrderRecorder(evCh)

	// Delete the shared resource first, then every dependent, concurrently.
	keys := append([]string{"ns/shared"}, leafKeys...)
	var wg sync.WaitGroup
	for _, k := range keys {
		wg.Add(1)
		go func(key string) {
			defer wg.Done()
			requestDelete(t, ctx, s, key)
		}(k)
	}
	wg.Wait()

	for _, k := range leafKeys {
		waitForGone(t, ctx, s, k)
	}
	waitForGone(t, ctx, s, "ns/shared")
	stopWatch()
	<-rec.done

	order := rec.snapshot()
	iShared := positionOf(order, "ns/shared")
	if iShared < 0 {
		t.Fatalf("shared resource must produce a physical delete event, got %v", order)
	}
	if iShared != len(order)-1 {
		t.Fatalf("shared resource must be the LAST physical deletion, got %v", order)
	}
	for _, lk := range leafKeys {
		if positionOf(order, lk) < 0 {
			t.Fatalf("%s missing from delete order %v", lk, order)
		}
		if positionOf(order, lk) >= iShared {
			t.Fatalf("%s must physically disappear before the shared resource, order=%v", lk, order)
		}
	}
	fmt.Printf("INPUT concurrent delete shared+%d dependents | JUDGE: shared last, physical delete order=%v\n",
		leaves, order)
}

// TestDeletingDependentStillBlocksParent asserts the presence rule directly:
// a dependent that is only MARKED for deletion (slowly cleaning up) still
// counts as present, so the depended-on resource must remain physically
// present AND stay observable as "deleting + guard attached". Once the
// dependent finally finishes, the parent follows automatically. A duplicate
// deletion request against the parent must not release it early.
func TestDeletingDependentStillBlocksParent(t *testing.T) {
	r := newDepRig(t, 4, 0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.ctrl.Run(ctx)
	s := r.store

	createObj(t, ctx, s, "ns", "parent")
	createObj(t, ctx, s, "ns", "child", object.DependencyRef{Name: "parent"})
	if !waitFor(30*time.Second, func() bool {
		return readyKey(t, ctx, s, "ns/parent") && readyKey(t, ctx, s, "ns/child")
	}) {
		t.Fatal("did not converge")
	}

	watchCtx, stopWatch := context.WithCancel(ctx)
	defer stopWatch()
	evCh, err := s.Watch(watchCtx)
	if err != nil {
		t.Fatal(err)
	}
	rec := newDeleteOrderRecorder(evCh)

	// Make the child's deletion slow: it stays present with a deletion
	// timestamp and its app finalizer for the duration of the hold.
	r.rec.setHoldDelete("ns/child", true)
	requestDelete(t, ctx, s, "ns/parent")
	requestDelete(t, ctx, s, "ns/child")

	// While the child is mid-cleanup, the parent must remain observable:
	// present, deleting, guard finalizer intact — across many delete passes.
	if !waitFor(30*time.Second, func() bool {
		po, err := s.Get(ctx, "ns/parent")
		return err == nil && po != nil && po.BeingDeleted() && po.HasFinalizer(GuardFinalizer)
	}) {
		t.Fatalf("parent must be preserved deleting+guarded while child is deleting")
	}
	time.Sleep(200 * time.Millisecond)
	po, _ := s.Get(ctx, "ns/parent")
	if po == nil || !po.BeingDeleted() || !po.HasFinalizer(GuardFinalizer) {
		t.Fatalf("parent must stay present/deleting/guarded while child slowly cleans up, got %v", po)
	}
	co, _ := s.Get(ctx, "ns/child")
	if co == nil || !co.BeingDeleted() {
		t.Fatalf("child must still be present in deleting state, got %v", co)
	}
	fmt.Printf("INPUT child deleting slowly | JUDGE: parent stays present, deleting=%t, guard=%v\n",
		po.BeingDeleted(), po.Finalizers)

	// Duplicate deletion requests (against the already-deleting parent) must
	// not release it early.
	requestDelete(t, ctx, s, "ns/parent")
	requestDelete(t, ctx, s, "ns/parent")
	time.Sleep(100 * time.Millisecond)
	if po2, _ := s.Get(ctx, "ns/parent"); po2 == nil || !po2.HasFinalizer(GuardFinalizer) {
		t.Fatalf("duplicate delete requests released the parent early: %v", po2)
	}

	// Let the child finish: parent must follow automatically (no manual
	// re-request), with the child physically gone first.
	r.rec.setHoldDelete("ns/child", false)
	waitForGone(t, ctx, s, "ns/child")
	waitForGone(t, ctx, s, "ns/parent")
	stopWatch()
	<-rec.done

	order := rec.snapshot()
	if positionOf(order, "ns/parent") <= positionOf(order, "ns/child") {
		t.Fatalf("parent must not physically disappear before the child, order=%v", order)
	}
	fmt.Printf("INPUT child released + duplicate parent deletes | JUDGE: auto-cascade, physical delete order=%v\n", order)
}
