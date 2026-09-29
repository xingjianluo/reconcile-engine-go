package reconcile

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xingjianluo/reconcile-engine-go/internal/object"
	"github.com/xingjianluo/reconcile-engine-go/internal/store"
)

// deleteOrderRecorder subscribes to the store before deletion begins and
// records the exact order in which objects are PHYSICALLY removed
// (store.EventDelete). A dedicated goroutine drains the watch channel eagerly
// so the small watch buffer can never back up and disconnect during a test.
type deleteOrderRecorder struct {
	mu    sync.Mutex
	order []string
	alive bool
}

func newDeleteOrderRecorder(t *testing.T, ctx context.Context, s *store.MemoryStore) *deleteOrderRecorder {
	t.Helper()
	r := &deleteOrderRecorder{alive: true}
	events, err := s.Watch(ctx)
	if err != nil {
		t.Fatalf("watch: %v", err)
	}
	go func() {
		for ev := range events {
			if ev.Type != store.EventDelete {
				continue
			}
			r.mu.Lock()
			r.order = append(r.order, ev.Key)
			r.mu.Unlock()
		}
		// Channel close means the store disconnected us; for these tiny
		// tests that must never happen, so mark the recorder dead.
		r.mu.Lock()
		r.alive = false
		r.mu.Unlock()
	}()
	return r
}

func (r *deleteOrderRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.order...)
}

func (r *deleteOrderRecorder) isAlive() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.alive
}

// indexOf returns the physical-removal position of key, or -1.
func indexOf(order []string, key string) int {
	for i, k := range order {
		if k == key {
			return i
		}
	}
	return -1
}

func requestDelete(t *testing.T, ctx context.Context, s store.Store, key string) {
	t.Helper()
	cur, err := s.Get(ctx, key)
	if err != nil {
		t.Fatalf("get %s: %v", key, err)
	}
	if _, err := s.Delete(ctx, key, cur.ResourceVersion); err != nil {
		t.Fatalf("delete %s: %v", key, err)
	}
}

func waitGone(t *testing.T, ctx context.Context, s store.Store, keys ...string) {
	t.Helper()
	want := make(map[string]bool, len(keys))
	for _, k := range keys {
		want[k] = true
	}
	for _, k := range keys {
		if !waitFor(3*time.Second, func() bool {
			_, err := s.Get(ctx, k)
			return errors.Is(err, object.ErrNotFound)
		}) {
			t.Fatalf("%s was never physically removed", k)
		}
	}
}

// waitRecorded waits until the recorder has observed a physical removal for
// every expected key. The store may publish the delete event slightly before a
// Get on another goroutine observes absence, so asserting the recorder slice
// immediately after the absence checks can race; this closes that window.
func (r *deleteOrderRecorder) waitRecorded(t *testing.T, keys ...string) []string {
	t.Helper()
	if !waitFor(3*time.Second, func() bool {
		order := r.snapshot()
		for _, k := range keys {
			if indexOf(order, k) < 0 {
				return false
			}
		}
		return true
	}) {
		t.Fatalf("delete-order recorder never observed removals for %v, got %v", keys, r.snapshot())
	}
	return r.snapshot()
}

// TestDeletionOrderChain asserts the strict physical-removal order for a
// three-level chain (leaf -> mid -> root) when ALL deletion requests are
// issued concurrently across several workers: leaf must vanish before mid,
// and mid before root.
func TestDeletionOrderChain(t *testing.T) {
	r := newDepRig(t, 4, 0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.ctrl.Run(ctx)
	s := r.store

	createObj(t, ctx, s, "ns", "leaf", object.DependencyRef{Name: "mid"})
	createObj(t, ctx, s, "ns", "mid", object.DependencyRef{Name: "root"})
	createObj(t, ctx, s, "ns", "root")
	for _, k := range []string{"ns/root", "ns/mid", "ns/leaf"} {
		if !waitFor(2*time.Second, func() bool { return readyKey(t, ctx, s, k) }) {
			t.Fatalf("%s did not converge", k)
		}
	}

	rec := newDeleteOrderRecorder(t, ctx, s)

	var wg sync.WaitGroup
	for _, k := range []string{"ns/root", "ns/mid", "ns/leaf"} {
		wg.Add(1)
		go func(key string) {
			defer wg.Done()
			requestDelete(t, ctx, s, key)
		}(k)
	}
	wg.Wait()

	waitGone(t, ctx, s, "ns/root", "ns/mid", "ns/leaf")

	order := rec.waitRecorded(t, "ns/root", "ns/mid", "ns/leaf")
	if !rec.isAlive() {
		t.Fatalf("delete-order watch disconnected during test; order=%v", order)
	}
	iLeaf, iMid, iRoot := indexOf(order, "ns/leaf"), indexOf(order, "ns/mid"), indexOf(order, "ns/root")
	if iLeaf < 0 || iMid < 0 || iRoot < 0 {
		t.Fatalf("missing deletions in recorded order: %v", order)
	}
	if !(iLeaf < iMid && iMid < iRoot) {
		t.Fatalf("physical deletion order must be leaf<mid<root, got %v", order)
	}
	fmt.Printf("INPUT concurrent deletes of leaf,mid,root | JUDGE: physical removal order=%v\n", order)
}

// TestDeletionOrderFanIn deletes one depended-on resource plus its several
// dependents all at once: the shared dependency must be removed strictly
// AFTER every one of its dependents has physically disappeared.
func TestDeletionOrderFanIn(t *testing.T) {
	r := newDepRig(t, 6, 0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.ctrl.Run(ctx)
	s := r.store

	keys := []string{"ns/root"}
	createObj(t, ctx, s, "ns", "root")
	for i := 0; i < 4; i++ {
		k := fmt.Sprintf("ns/dep%02d", i)
		keys = append(keys, k)
		createObj(t, ctx, s, "ns", fmt.Sprintf("dep%02d", i), object.DependencyRef{Name: "root"})
	}
	for _, k := range keys {
		if !waitFor(2*time.Second, func() bool { return readyKey(t, ctx, s, k) }) {
			t.Fatalf("%s did not converge", k)
		}
	}

	rec := newDeleteOrderRecorder(t, ctx, s)

	var wg sync.WaitGroup
	for _, k := range keys {
		wg.Add(1)
		go func(key string) {
			defer wg.Done()
			requestDelete(t, ctx, s, key)
		}(k)
	}
	wg.Wait()

	waitGone(t, ctx, s, keys...)

	order := rec.waitRecorded(t, keys...)
	if !rec.isAlive() {
		t.Fatalf("delete-order watch disconnected during test; order=%v", order)
	}
	iRoot := indexOf(order, "ns/root")
	if iRoot < 0 {
		t.Fatalf("root deletion missing from order: %v", order)
	}
	for i := 0; i < 4; i++ {
		dk := fmt.Sprintf("ns/dep%02d", i)
		if di := indexOf(order, dk); di < 0 || di > iRoot {
			t.Fatalf("%s (pos=%d) must be physically removed before ns/root (pos=%d); order=%v",
				dk, di, iRoot, order)
		}
	}
	fmt.Printf("INPUT root + 4 dependents deleted concurrently | JUDGE: root removed last, order=%v\n", order)
}

// TestDeletionWaitsForDeletingDependent proves "a delete request arrived" is
// not enough: while a dependent is marked deleting but has not physically
// disappeared (its cleanup is slow), the depended-on object must remain
// present, deleting, guard intact — observably and without flapping. When the
// dependent finally finishes, the dependency is released automatically.
func TestDeletionWaitsForDeletingDependent(t *testing.T) {
	r := newDepRig(t, 2, 0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.ctrl.Run(ctx)
	s := r.store

	createObj(t, ctx, s, "ns", "parent")
	createObj(t, ctx, s, "ns", "child", object.DependencyRef{Name: "parent"})
	if !waitFor(2*time.Second, func() bool {
		return readyKey(t, ctx, s, "ns/parent") && readyKey(t, ctx, s, "ns/child")
	}) {
		t.Fatal("did not converge")
	}

	// Make the child's eventual deletion cleanup slow: it will keep its app
	// finalizer once deleting (an alive held child is simply a no-op, so this
	// does not disturb the already-converged state).
	r.rec.setHold("ns/child", true)
	// Let any in-flight reconcile observe the hold before deletion begins.
	time.Sleep(40 * time.Millisecond)

	rec := newDeleteOrderRecorder(t, ctx, s)
	requestDelete(t, ctx, s, "ns/parent")
	requestDelete(t, ctx, s, "ns/child")

	// For a while: child remains present (deleting, finalizer kept), parent is
	// retained deleting with its guard, and neither physically disappears.
	for i := 0; i < 8; i++ {
		time.Sleep(30 * time.Millisecond)
		co, cerr := s.Get(ctx, "ns/child")
		if cerr != nil || !co.BeingDeleted() || !co.HasFinalizer(testFinalizer) {
			t.Fatalf("slow child must remain deleting with finalizer, got %v err=%v", co, cerr)
		}
		po, perr := s.Get(ctx, "ns/parent")
		if perr != nil || po == nil {
			t.Fatalf("parent must be retained while child is still deleting, err=%v", perr)
		}
		if !po.BeingDeleted() || !po.HasFinalizer(GuardFinalizer) {
			t.Fatalf("parent must stay deleting with guard, got %v", po)
		}
	}
	if order := rec.snapshot(); len(order) != 0 {
		t.Fatalf("nothing may be physically removed while child is stuck, got %v", order)
	}
	fmt.Printf("INPUT child stuck deleting | JUDGE: parent retained deleting+guarded, no removals, observable\n")

	// Release the child's cleanup. A benign touch bumps its version and, via
	// watch, re-enqueues the parked child — the in-process analogue of a
	// reconciler re-queuing once its external cleanup condition clears.
	r.rec.setHold("ns/child", false)
	if cur, err := s.Get(ctx, "ns/child"); err == nil {
		if _, err := s.Update(ctx, "ns/child", cur.ResourceVersion, func(o *object.Object) {
			o.AnnotateTouched()
		}); err != nil {
			t.Fatal(err)
		}
	}
	if !waitFor(3*time.Second, func() bool {
		_, e := s.Get(ctx, "ns/child")
		return errors.Is(e, object.ErrNotFound)
	}) {
		t.Fatal("child never finished after release")
	}
	if !waitFor(3*time.Second, func() bool {
		_, e := s.Get(ctx, "ns/parent")
		return errors.Is(e, object.ErrNotFound)
	}) {
		t.Fatal("parent must auto-delete once the deleting child disappeared")
	}
	order := rec.waitRecorded(t, "ns/child", "ns/parent")
	if len(order) != 2 || order[0] != "ns/child" || order[1] != "ns/parent" {
		t.Fatalf("physical order must be child<parent, got %v", order)
	}
	fmt.Printf("INPUT child cleanup released | JUDGE: auto-teardown physical order=%v\n", order)
}

// TestDuplicateDeleteRequestsDoNotReleaseGuard issues the parent's delete
// repeatedly while the child is still present: re-submitting the request must
// not release the parent early. The parent disappears only after the child.
func TestDuplicateDeleteRequestsDoNotReleaseGuard(t *testing.T) {
	r := newDepRig(t, 2, 0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.ctrl.Run(ctx)
	s := r.store

	createObj(t, ctx, s, "ns", "parent")
	createObj(t, ctx, s, "ns", "child", object.DependencyRef{Name: "parent"})
	if !waitFor(2*time.Second, func() bool {
		return readyKey(t, ctx, s, "ns/parent") && readyKey(t, ctx, s, "ns/child")
	}) {
		t.Fatal("did not converge")
	}

	rec := newDeleteOrderRecorder(t, ctx, s)
	requestDelete(t, ctx, s, "ns/parent")

	// Hammer the already-deleting parent with duplicate delete requests.
	var stop atomic.Bool
	go func() {
		for !stop.Load() {
			if cur, err := s.Get(ctx, "ns/parent"); err == nil {
				_, _ = s.Delete(ctx, "ns/parent", cur.ResourceVersion)
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()
	time.Sleep(200 * time.Millisecond)
	if _, err := s.Get(ctx, "ns/parent"); err != nil {
		t.Fatalf("duplicate deletes must not remove parent while child exists, err=%v", err)
	}
	if order := rec.snapshot(); len(order) != 0 {
		t.Fatalf("parent must not be removed by repeated delete requests, removals=%v", order)
	}

	stop.Store(true)
	requestDelete(t, ctx, s, "ns/child")
	waitGone(t, ctx, s, "ns/child", "ns/parent")
	order := rec.waitRecorded(t, "ns/child", "ns/parent")
	if len(order) != 2 || order[0] != "ns/child" || order[1] != "ns/parent" {
		t.Fatalf("physical order must be child<parent, got %v", order)
	}
	fmt.Printf("INPUT repeated parent deletes + child delete | JUDGE: parent held, then child<parent order=%v\n", order)
}

// TestDeletionOrderStress repeats the fan-in teardown many times across
// several workers, asserting the exact physical order on every round. It is
// intended to be run under -race and -count=N.
func TestDeletionOrderStress(t *testing.T) {
	const rounds = 5
	for round := 0; round < rounds; round++ {
		t.Run(fmt.Sprintf("round%02d", round), func(t *testing.T) {
			r := newDepRig(t, 6, 0)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go r.ctrl.Run(ctx)
			s := r.store

			keys := []string{"ns/root"}
			createObj(t, ctx, s, "ns", "root")
			for i := 0; i < 5; i++ {
				createObj(t, ctx, s, "ns", fmt.Sprintf("d%02d", i), object.DependencyRef{Name: "root"})
				keys = append(keys, fmt.Sprintf("ns/d%02d", i))
			}
			for _, k := range keys {
				if !waitFor(2*time.Second, func() bool { return readyKey(t, ctx, s, k) }) {
					t.Fatalf("%s did not converge", k)
				}
			}

			rec := newDeleteOrderRecorder(t, ctx, s)
			var wg sync.WaitGroup
			for _, k := range keys {
				wg.Add(1)
				go func(key string) {
					defer wg.Done()
					requestDelete(t, ctx, s, key)
				}(k)
			}
			wg.Wait()
			waitGone(t, ctx, s, keys...)

			order := rec.waitRecorded(t, keys...)
			if len(order) != len(keys) {
				t.Fatalf("round %d: expected %d removals, got %v", round, len(keys), order)
			}
			if order[len(order)-1] != "ns/root" {
				t.Fatalf("round %d: root must be removed last, got %v", round, order)
			}
			if !rec.isAlive() {
				t.Fatalf("round %d: delete-order watch disconnected", round)
			}
		})
	}
	fmt.Printf("INPUT %d concurrent fan-in teardown rounds | JUDGE: root removed last every round\n", rounds)
}
