// Command demo drives the reconciliation engine through create, repeated
// update (coalescing), version conflict, failing reconcile (retry/backoff)
// and finalizer-protected deletion, all against an in-memory store with no
// external services or credentials.
package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xingjianluo/reconcile-engine-go/internal/object"
	"github.com/xingjianluo/reconcile-engine-go/internal/queue"
	"github.com/xingjianluo/reconcile-engine-go/internal/reconcile"
	"github.com/xingjianluo/reconcile-engine-go/internal/store"
)

const finalizerName = "demo.io/cleanup"

// appReconciler is an idempotent reconciler. On active objects it ensures
// its finalizer and mirrors spec into status (the "actual state"), recording
// the observed spec of each declared dependency. On deleting objects it
// performs cleanup once, removes its finalizer, and relies on a follow-up
// reconcile to physically remove the object. It also simulates transient
// failures and version conflicts.
type appReconciler struct {
	s     store.Store
	flaky int32 // first N active reconciles fail
	// frozenKey, when non-empty, names a key that must stay unconverged
	// (active reconciles requeue without acting) until frozenKey is cleared.
	freezeMu  sync.Mutex
	frozenKey string
	calls     atomic.Int64
}

// releaseFreeze unblocks a key held out of readiness.
func (r *appReconciler) releaseFreeze() {
	r.freezeMu.Lock()
	r.frozenKey = ""
	r.freezeMu.Unlock()
}

func (r *appReconciler) Reconcile(ctx context.Context, obj *object.Object) (reconcile.Result, error) {
	n := r.calls.Add(1)
	fmt.Printf("    [reconcile #%d] input: %s spec=%v status=%v deps=%v\n",
		n, obj.String(), obj.Spec, obj.Status, obj.DependsOn)

	if obj.BeingDeleted() {
		// Deletion phase: run idempotent cleanup then drop the finalizer.
		if obj.HasFinalizer(finalizerName) {
			fmt.Printf("    [reconcile #%d] judgment: deleting -> run cleanup, remove finalizer (rv=%d)\n", n, obj.ResourceVersion)
			_, err := r.s.Update(ctx, obj.Key(), obj.ResourceVersion, func(o *object.Object) {
				o.RemoveFinalizer(finalizerName)
			})
			if err != nil {
				return reconcile.Result{}, err
			}
			return reconcile.Result{}, nil
		}
		// No finalizer left and still present -> safe to remove physically.
		fmt.Printf("    [reconcile #%d] judgment: no finalizers -> physical remove\n", n)
		if err := r.s.Remove(ctx, obj.Key(), obj.ResourceVersion); err != nil {
			return reconcile.Result{}, err
		}
		return reconcile.Result{}, nil
	}

	// Simulate a couple of transient failures to show backoff retry.
	if r.flaky > 0 && atomic.AddInt32(&r.flaky, -1) > 0 {
		return reconcile.Result{}, errors.New("simulated transient downstream failure")
	}

	// Frozen keys deliberately stay unconverged to demo dependency waiting.
	r.freezeMu.Lock()
	frozen := r.frozenKey == obj.Key()
	r.freezeMu.Unlock()
	if frozen {
		fmt.Printf("    [reconcile #%d] judgment: %s intentionally held not-ready\n", n, obj.Key())
		return reconcile.Result{Requeue: true}, nil
	}

	// Active phase: ensure finalizer + converge status to spec idempotently.
	status := cloneMap(obj.Spec)
	for _, dep := range obj.DependsOn {
		d, err := r.s.Get(ctx, dep)
		if err != nil {
			return reconcile.Result{}, err
		}
		status["dep:"+dep] = d.Spec["rev"]
	}
	changed := !obj.HasFinalizer(finalizerName) || !mapsEqual(status, obj.Status)
	if !changed {
		fmt.Printf("    [reconcile #%d] judgment: already converged, no side effect\n", n)
		return reconcile.Result{}, nil
	}
	_, err := r.s.Update(ctx, obj.Key(), obj.ResourceVersion, func(o *object.Object) {
		o.AddFinalizer(finalizerName)
		o.Status = status
	})
	if err != nil {
		// A conflict means we based the write on a stale version; the
		// controller will re-read and retry.
		return reconcile.Result{}, err
	}
	fmt.Printf("    [reconcile #%d] judgment: applied desired state (status=spec, finalizer added)\n", n)
	return reconcile.Result{}, nil
}

func mapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func cloneMap(m map[string]string) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := store.NewMemoryStore()
	q := queue.New(queue.Config{
		Backoff:     queue.NewExponentialBackoff(5*time.Millisecond, 100*time.Millisecond),
		MaxAttempts: 6,
	})
	rec := &appReconciler{s: s, flaky: 2}
	ctrl := reconcile.New(reconcile.Config{
		Store:         s,
		Queue:         q,
		Reconciler:    rec,
		Workers:       2,
		ShutdownGrace: 2 * time.Second,
		Logger:        func(f string, a ...any) { fmt.Printf(f+"\n", a...) },
	})

	go func() {
		if err := ctrl.Run(ctx); err != nil {
			fmt.Println("controller error:", err)
		}
	}()

	key := object.MakeKey("default", "widget")
	mustCreate(s, &object.Object{Namespace: "default", Name: "widget", Spec: map[string]string{"rev": "v1"}})
	time.Sleep(120 * time.Millisecond)

	// Burst of updates coalesces; each CAS uses the version observed at the
	// start of that reconcile, demonstrating conflict + re-read.
	for i := 2; i <= 5; i++ {
		cur := mustGet(s, key)
		_, _ = s.Update(ctx, key, cur.ResourceVersion, func(o *object.Object) {
			o.Spec["rev"] = fmt.Sprintf("v%d", i)
		})
	}
	time.Sleep(200 * time.Millisecond)

	final := mustGet(s, key)
	fmt.Printf("\n>>> converged status: %v finalizers=%v rv=%d\n\n",
		final.Status, final.Finalizers, final.ResourceVersion)

	// ---- Dependency orchestration --------------------------------------
	demoDependencies(ctx, s, ctrl, rec)

	// Deletion: timestamp set, finalizer cleanup, then physical removal.
	casDelete(s, key)
	time.Sleep(150 * time.Millisecond)
	if _, err := s.Get(ctx, key); errors.Is(err, object.ErrNotFound) {
		fmt.Println(">>> object physically removed after finalizer cleared")
	} else {
		fmt.Println(">>> ERROR: object still present")
	}

	cancel()
	time.Sleep(300 * time.Millisecond)
	fmt.Println(">>> demo complete; terminal failures:", ctrl.Failures())
}

func mustCreate(s store.Store, o *object.Object) {
	if _, err := s.Create(context.Background(), o); err != nil {
		panic(err)
	}
}

func mustGet(s store.Store, key string) *object.Object {
	o, err := s.Get(context.Background(), key)
	if err != nil {
		panic(err)
	}
	return o
}

// casDelete marks a key for deletion, re-reading on optimistic conflict.
func casDelete(s store.Store, key string) {
	for i := 0; i < 100; i++ {
		cur := mustGet(s, key)
		if _, err := s.Delete(context.Background(), key, cur.ResourceVersion); err == nil {
			return
		}
		time.Sleep(time.Millisecond)
	}
	panic("casDelete kept conflicting for " + key)
}

// waitGone polls until a key is physically absent.
func waitGone(s store.Store, key string, d time.Duration) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if _, err := s.Get(context.Background(), key); errors.Is(err, object.ErrNotFound) {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	_, err := s.Get(context.Background(), key)
	return errors.Is(err, object.ErrNotFound)
}

// demoDependencies exercises dependency gating, change propagation,
// missing/cycle conclusions, and reverse-dependency-ordered deletion.
func demoDependencies(ctx context.Context, s store.Store, ctrl *reconcile.Controller, rec *appReconciler) {
	fmt.Println("========== dependency orchestration ==========")

	dbKey := object.MakeKey("default", "database")
	appKey := object.MakeKey("default", "app")
	orphanKey := object.MakeKey("default", "orphan")
	cycA := object.MakeKey("default", "cyc-a")
	cycB := object.MakeKey("default", "cyc-b")

	// 1) An app whose dependency exists but is not yet converged is held
	//    quietly (no failure, no side effect), then proceeds on readiness.
	rec.freezeMu.Lock()
	rec.frozenKey = dbKey
	rec.freezeMu.Unlock()
	mustCreate(s, &object.Object{
		Namespace: "default", Name: "database",
		Spec: map[string]string{"rev": "d1"},
	})
	mustCreate(s, &object.Object{
		Namespace: "default", Name: "app",
		Spec:      map[string]string{"rev": "v1"},
		DependsOn: []string{dbKey},
	})
	time.Sleep(150 * time.Millisecond)
	if o := mustGet(s, appKey); len(o.Finalizers) != 0 {
		fmt.Println(">>> ERROR: app acted before dependency was ready")
	}
	if f := ctrl.Failures()[appKey]; f != nil {
		fmt.Printf(">>> ERROR: waiting was recorded as failure: %v\n", f)
	}
	fmt.Println(">>> INPUT app depends on existing-but-unready database | JUDGE: held quietly, no failure")
	rec.releaseFreeze()
	// Touch the database so it re-reconciles now that it can converge.
	cur := mustGet(s, dbKey)
	_, _ = s.Update(ctx, dbKey, cur.ResourceVersion, func(o *object.Object) {})
	time.Sleep(200 * time.Millisecond)
	if app := mustGet(s, appKey); len(app.Finalizers) == 0 {
		fmt.Println(">>> ERROR: app did not proceed after dependency became ready")
	} else {
		fmt.Printf(">>> JUDGE: database ready -> app auto-proceeded status=%v\n", app.Status)
	}

	// 2) Missing dependency -> distinguishable terminal conclusion.
	mustCreate(s, &object.Object{
		Namespace: "default", Name: "orphan",
		Spec:      map[string]string{"rev": "v1"},
		DependsOn: []string{"default/ghost"},
	})
	time.Sleep(120 * time.Millisecond)
	fmt.Printf(">>> JUDGE: orphan terminal failure = %v\n", ctrl.Failures()[orphanKey])

	// 3) Cycle -> distinguishable terminal conclusion (no deadlock).
	mustCreate(s, &object.Object{
		Namespace: "default", Name: "cyc-a",
		Spec: map[string]string{"rev": "v1"}, DependsOn: []string{cycB},
	})
	mustCreate(s, &object.Object{
		Namespace: "default", Name: "cyc-b",
		Spec: map[string]string{"rev": "v1"}, DependsOn: []string{cycA},
	})
	time.Sleep(120 * time.Millisecond)
	fmt.Printf(">>> JUDGE: cyc-a failure = %v\n", ctrl.Failures()[cycA])
	fmt.Printf(">>> JUDGE: cyc-b failure = %v\n", ctrl.Failures()[cycB])

	// 4) Change the dependency: propagation drives the app to the new state.
	dbCur := mustGet(s, dbKey)
	_, _ = s.Update(ctx, dbKey, dbCur.ResourceVersion, func(o *object.Object) {
		o.Spec["rev"] = "d2"
	})
	time.Sleep(200 * time.Millisecond)
	app := mustGet(s, appKey)
	fmt.Printf(">>> INPUT database rev d1->d2 | JUDGE: app re-reconciled, dep record=%q\n",
		app.Status["dep:"+dbKey])

	// 6) Ordered deletion: delete dependency first, then the dependent.
	casDelete(s, dbKey)
	time.Sleep(120 * time.Millisecond)
	if _, err := s.Get(ctx, dbKey); errors.Is(err, object.ErrNotFound) {
		fmt.Println(">>> ERROR: database removed while app still exists")
	} else {
		fmt.Println(">>> INPUT delete database while app exists | JUDGE: database blocked, still present")
	}
	casDelete(s, appKey)
	if !waitGone(s, appKey, 2*time.Second) {
		fmt.Println(">>> ERROR: app not removed")
	}
	if !waitGone(s, dbKey, 2*time.Second) {
		fmt.Println(">>> ERROR: database not removed after app cleanup")
	} else {
		fmt.Println(">>> JUDGE: app removed first, database removed only afterwards")
	}

	// Structural-failure objects are cleaned up directly for demo teardown.
	for _, k := range []string{orphanKey, cycA, cycB} {
		if _, err := s.Get(ctx, k); err == nil {
			forceRemove(s, k)
		}
	}
	fmt.Println("========== dependency demo complete ==========")
}

// forceRemove wipes an object's finalizers and deletes it (demo teardown).
func forceRemove(s store.Store, key string) {
	cur := mustGet(s, key)
	if _, err := s.Update(context.Background(), key, cur.ResourceVersion, func(o *object.Object) {
		o.Finalizers = nil
	}); err != nil {
		return
	}
	casDelete(s, key)
}
