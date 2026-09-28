package reconcile

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xingjianluo/reconcile-engine-go/internal/dependency"
	"github.com/xingjianluo/reconcile-engine-go/internal/object"
	"github.com/xingjianluo/reconcile-engine-go/internal/queue"
	"github.com/xingjianluo/reconcile-engine-go/internal/store"
)

// immediateRequeueDelay yields near-immediate reprocessing after an engine
// metadata write (guard-finalizer add/remove) while avoiding a pathological
// spin; it never counts against the backoff attempt budget.
const immediateRequeueDelay = time.Millisecond

// Config configures a Controller.
type Config struct {
	Store         store.Store
	Queue         queue.Queue
	Reconciler    Reconciler
	Workers       int
	ShutdownGrace time.Duration
	Logger        func(format string, args ...any)

	// IsReady judges whether an object has converged to a state that may
	// unblock its dependents. Defaults to dependency.DefaultReady (Ready flag
	// set and not being deleted).
	IsReady func(*object.Object) bool
	// DependencyWaitDelay is how long a key whose dependencies are not all
	// ready yet waits quietly before being retried. Such waits never consume
	// the backoff attempt budget and are never terminal failures.
	DependencyWaitDelay time.Duration
}

// Controller distributes store events to the queue and runs workers that
// process each resource's changes serially and in version order.
type Controller struct {
	cfg Config

	graph *dependency.Graph

	// bootstrapped records whether the first full-list snapshot has been
	// taken; only pumpLoop touches it (single goroutine).
	bootstrapped bool

	inFlight atomic.Int64
	mu       sync.Mutex
	failures map[string]error
}

// New constructs a Controller.
func New(cfg Config) *Controller {
	if cfg.Workers <= 0 {
		cfg.Workers = 1
	}
	if cfg.ShutdownGrace <= 0 {
		cfg.ShutdownGrace = 5 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = func(string, ...any) {}
	}
	if cfg.IsReady == nil {
		cfg.IsReady = dependency.DefaultReady
	}
	if cfg.DependencyWaitDelay <= 0 {
		cfg.DependencyWaitDelay = 25 * time.Millisecond
	}
	return &Controller{
		cfg:      cfg,
		graph:    dependency.NewGraph(),
		failures: make(map[string]error),
	}
}

// log prefixes lines so test/demo output is traceable.
func (c *Controller) log(format string, args ...any) {
	c.cfg.Logger("    [controller] "+format, args...)
}

// Run starts the event pump and workers, blocking until ctx is cancelled.
// On cancellation it stops pulling new events, lets the queue drain its
// ready work and tracks in-flight reconciles so they remain observable.
func (c *Controller) Run(ctx context.Context) error {
	var wg sync.WaitGroup

	// Event pump owns the watch lifecycle: it establishes the watch, lists
	// the initial snapshot, and — if the store ever disconnects a slow
	// watcher — re-establishes the watch and performs a full resync.
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.pumpLoop(ctx)
	}()

	workerCtx, stopWorkers := context.WithCancel(context.Background())
	for i := 0; i < c.cfg.Workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			c.worker(workerCtx, id)
		}(i)
	}

	<-ctx.Done()
	c.log("shutdown signal received; stopping event intake")

	// Bound the drain: the queue waits for tracked keys while in-flight
	// reconciles remain observable via InFlight, then workers are stopped.
	drainCtx, cancelDrain := context.WithTimeout(context.Background(), c.cfg.ShutdownGrace)
	defer cancelDrain()
	drainErr := c.cfg.Queue.Shutdown(drainCtx)
	stopWorkers()
	wg.Wait()
	if drainErr != nil {
		c.log("drain incomplete: %v (in-flight was observable meanwhile)", drainErr)
	} else {
		c.log("drained cleanly; in-flight=%d", c.InFlight())
	}
	return nil
}

// pumpLoop maintains the watch, the dependency graph and the bounded queue.
//
// Watch lifecycle: the store disconnects a slow subscriber (closing its
// channel) rather than buffering without bound. That is a recoverable
// "resync needed" condition, not shutdown: pumpLoop re-establishes a watch,
// lists the whole store, rebuilds the graph snapshot and enqueues every live
// object, then keeps pumping.
//
// Admission is the queue's bounded, blocking Add (blocks — never drops — when
// saturated). Blocking the pump cannot deadlock: workers do not depend on the
// pump to make progress, and any reconcile writes produced while the pump is
// blocked either land in the watch buffer or trigger a disconnect that the
// reconnect+resync below fully recovers. As workers retire keys, slots free
// and the blocked Add resumes.
func (c *Controller) pumpLoop(ctx context.Context) {
	admit := func(key string, opts queue.EnqueueOptions) {
		c.clearFailure(key)
		c.cfg.Queue.Add(key, opts)
	}

	handleEvent := func(ev store.Event) {
		switch ev.Type {
		case store.EventUpsert:
			if ev.Object == nil {
				return
			}
			deps := dependency.NormalizeRefs(ev.Object.Namespace, ev.Object.Dependencies)
			// Newly referenced upstream objects must be triggered so they can
			// attach their reverse-deletion guard even if they finished their
			// own gating before this edge entered the graph.
			addedUpstream := c.graph.Upsert(ev.Key, deps, c.cfg.IsReady(ev.Object), ev.Object.BeingDeleted())
			admit(ev.Key, queue.EnqueueOptions{
				ResourceVersion: ev.Object.ResourceVersion,
				HasVersion:      true,
			})
			// Downstream: any object that depends on the changed key.
			for _, dk := range c.graph.Dependents(ev.Key) {
				admit(dk, queue.EnqueueOptions{})
			}
			// Upstream: newly referenced dependencies (guard maintenance).
			for _, up := range addedUpstream {
				if up != ev.Key {
					admit(up, queue.EnqueueOptions{})
				}
			}
		case store.EventDelete:
			dependents := c.graph.Delete(ev.Key)
			admit(ev.Key, queue.EnqueueOptions{})
			for _, dk := range dependents {
				admit(dk, queue.EnqueueOptions{})
			}
		}
	}

	// resync rebuilds the graph from a full list and re-enqueues every live
	// object, recovering any events missed while the watch was disconnected.
	resync := func(reason string) {
		c.log("watch %s; resyncing from full list", reason)
		objs, err := c.cfg.Store.List(ctx)
		if err != nil {
			if ctx.Err() == nil {
				c.log("resync list failed: %v", err)
			}
			return
		}
		entries := make([]dependency.SnapshotEntry, 0, len(objs))
		for _, o := range objs {
			entries = append(entries, c.snapshotEntry(o))
		}
		c.graph.Replace(entries)
		for _, o := range objs {
			admit(o.Key(), queue.EnqueueOptions{
				ResourceVersion: o.ResourceVersion,
				HasVersion:      true,
			})
		}
	}

	for ctx.Err() == nil {
		watchCtx, cancelWatch := context.WithCancel(ctx)
		events, werr := c.cfg.Store.Watch(watchCtx)
		if werr != nil {
			cancelWatch()
			if ctx.Err() != nil {
				return
			}
			c.log("watch establish failed: %v; retrying", werr)
			if !sleep(ctx, c.cfg.DependencyWaitDelay) {
				return
			}
			continue
		}

		// Initial snapshot: list AFTER establishing the watch, then replay
		// any events that arrived in the interim, so no change is missed.
		if !c.bootstrapped {
			c.bootstrapped = true
			resync("established (initial list)")
		}

		disconnected := false
		func() {
			defer cancelWatch()
			for ctx.Err() == nil {
				select {
				case <-ctx.Done():
					return
				case ev, ok := <-events:
					if !ok {
						// Slow-consumer disconnect: mark for resync after the
						// old subscription is released.
						disconnected = true
						return
					}
					handleEvent(ev)
				}
			}
		}()
		if ctx.Err() != nil {
			return
		}
		if disconnected {
			resync("disconnected; rebuilding")
		}
	}
}

// sleep waits for d or until ctx is cancelled; reports whether d elapsed.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// snapshotEntry converts a stored object into a graph snapshot entry.
func (c *Controller) snapshotEntry(o *object.Object) dependency.SnapshotEntry {
	return dependency.SnapshotEntry{
		Key:      o.Key(),
		Deps:     dependency.NormalizeRefs(o.Namespace, o.Dependencies),
		Ready:    c.cfg.IsReady(o),
		Deleting: o.BeingDeleted(),
	}
}

// worker pulls ready keys and reconciles them until the queue shuts down.
func (c *Controller) worker(ctx context.Context, id int) {
	for {
		key, ok := c.cfg.Queue.Get(ctx)
		if !ok {
			return
		}
		c.processKey(ctx, id, key)
	}
}

// processKey performs exactly one convergence attempt for a key. It always
// fetches the freshest object first, so any optimistic conflict inside a
// reconciler is handled by re-reading and retrying rather than by acting on
// stale data.
//
// Before invoking the app reconciler it enforces dependency ordering:
// deleting objects follow reverse-order finalizer rules; alive objects are
// blocked (quiet wait, distinguishable failure, or guard-finalizer write)
// according to the dependency graph.
func (c *Controller) processKey(ctx context.Context, id int, key string) {
	c.inFlight.Add(1)
	defer c.inFlight.Add(-1)

	obj, err := c.cfg.Store.Get(ctx, key)
	if err != nil {
		if errors.Is(err, object.ErrNotFound) {
			// Object already gone (e.g. final finalize observed delete).
			c.log("worker %d: key %s absent -> done", id, key)
			c.cfg.Queue.Done(key)
			return
		}
		c.fail(key, ReasonReconcileError, 0, err)
		c.cfg.Queue.Fail(key, err)
		return
	}

	// Deletion always takes the reverse-order path: an object with live
	// dependents is preserved until they have disappeared.
	if obj.BeingDeleted() {
		if cont := c.processDeletion(ctx, id, obj); !cont {
			return
		}
	} else {
		// Alive: dependency gate. A false result means the key was already
		// resolved (waited/failed/re-queued) and must not run now.
		if cont := c.gateActive(ctx, id, obj); !cont {
			return
		}
	}

	c.log("worker %d: reconciling %s", id, obj.String())
	res, rerr := c.cfg.Reconciler.Reconcile(ctx, obj)

	if rerr == nil {
		if res.Requeue {
			// Explicit request to run again after a short delay (not an
			// error); the merged newest version is preserved.
			c.cfg.Queue.RequeueWith(key, 20*time.Millisecond)
			return
		}
		c.cfg.Queue.Done(key)
		return
	}

	// A conflict means the object changed under us: re-read on the next try.
	if store.IsConflict(rerr) {
		c.log("worker %d: conflict on %s (%v) -> requeue after re-read", id, key, rerr)
		term := c.cfg.Queue.Requeue(key, rerr)
		c.recordIfTerminal(key, ReasonConflictLoop, term)
		return
	}

	c.log("worker %d: error on %s (%v) -> backoff retry", id, key, rerr)
	term := c.cfg.Queue.Requeue(key, rerr)
	c.recordIfTerminal(key, ReasonRetryExhausted, term)
}

// gateActive enforces dependency ordering for an alive object. It returns
// true only when the app reconciler is allowed to run now.
func (c *Controller) gateActive(ctx context.Context, id int, obj *object.Object) bool {
	key := obj.Key()

	// 1) A cycle can never resolve under the current declarations: record a
	//    distinguishable terminal conclusion instead of waiting forever.
	if loop := c.graph.Cycle(key); loop != nil {
		err := &dependency.CycleError{Key: key, Cycle: loop}
		c.log("worker %d: %v -> terminal", id, err)
		c.fail(key, ReasonDependencyCycle, 0, err)
		c.cfg.Queue.Fail(key, err)
		return false
	}

	// 2) Referencing a non-existent object is a distinct conclusion, not an
	//    unbounded wait.
	missing, notReady := c.graph.Blockers(key)
	if len(missing) > 0 {
		err := &dependency.MissingError{Key: key, Missing: missing}
		c.log("worker %d: %v -> terminal until created", id, err)
		c.fail(key, ReasonDependencyMissing, 0, err)
		c.cfg.Queue.Fail(key, err)
		return false
	}

	// 3) Dependencies exist but are not all ready yet: wait quietly. This is
	//    neither an error nor an exhausted retry; readiness propagation will
	//    enqueue the key again, and the delay bounds re-evaluation churn. The
	//    guard finalizer is NOT touched here, so waiting cannot churn it.
	if len(notReady) > 0 {
		c.log("worker %d: %s waiting on %d not-ready dependency/ies %v",
			id, key, len(notReady), notReady)
		c.cfg.Queue.RequeueWith(key, c.cfg.DependencyWaitDelay)
		return false
	}

	// 4) All dependencies are ready: ensure the reverse-deletion guard is
	//    present while live dependents exist. The guard is monotonic during
	//    the object's lifetime (added at most once; removed only on the
	//    deletion path), so this can never oscillate or fan out repeatedly.
	if cont := c.ensureGuardFinalizer(ctx, id, obj); !cont {
		return false
	}
	return true
}

// ensureGuardFinalizer adds the engine guard finalizer exactly once when the
// object currently has live dependents. It returns false when a guard write
// was performed (or conflicted) and the key was re-queued to re-read. It never
// removes a guard from an alive object: removal happens only during deletion,
// which keeps the guard monotonic and the system free of add/remove churn.
func (c *Controller) ensureGuardFinalizer(ctx context.Context, id int, obj *object.Object) bool {
	key := obj.Key()
	if obj.HasFinalizer(GuardFinalizer) || len(c.graph.Dependents(key)) == 0 {
		return true
	}
	out, err := c.cfg.Store.Update(ctx, key, obj.ResourceVersion, func(o *object.Object) {
		o.AddFinalizer(GuardFinalizer)
	})
	if err != nil {
		// Conflict: the object moved on; re-read on the next attempt.
		c.log("worker %d: guard-add conflict on %s (%v) -> re-read", id, key, err)
	} else {
		c.log("worker %d: added dependency guard finalizer to %s (rv=%d)", id, key, out.ResourceVersion)
	}
	c.cfg.Queue.RequeueWith(key, immediateRequeueDelay)
	return false
}

// processDeletion enforces reverse-order deletion. It returns true only when
// the object is allowed to proceed to the app reconciler (finalizer cleanup
// and physical removal).
func (c *Controller) processDeletion(ctx context.Context, id int, obj *object.Object) bool {
	key := obj.Key()

	// Deletion safety is decided against the authoritative store, not only the
	// (derived, briefly-asynchronous) reverse index: an object may be removed
	// only once no LIVE object still references it. A dependent that is itself
	// deleting does not block it, so a chain tears down leaf-first.
	blocked, err := c.hasLiveDependent(ctx, key)
	if err != nil {
		c.log("worker %d: dependent scan failed for deleting %s (%v) -> wait", id, key, err)
		c.cfg.Queue.RequeueWith(key, c.cfg.DependencyWaitDelay)
		return false
	}
	if blocked {
		c.log("worker %d: %s deleting but still has a live dependent -> keep guard, wait", id, key)
		c.cfg.Queue.RequeueWith(key, c.cfg.DependencyWaitDelay)
		return false
	}

	// No live dependent remains: drop the engine guard first (if present), then
	// let the app reconciler perform its own cleanup on a subsequent run.
	if obj.HasFinalizer(GuardFinalizer) {
		_, err := c.cfg.Store.Update(ctx, key, obj.ResourceVersion, func(o *object.Object) {
			o.RemoveFinalizer(GuardFinalizer)
		})
		if err != nil {
			c.log("worker %d: guard-remove conflict during delete on %s (%v) -> re-read", id, key, err)
		} else {
			c.log("worker %d: live dependents cleared, removed guard from deleting %s", id, key)
		}
		c.cfg.Queue.RequeueWith(key, immediateRequeueDelay)
		return false
	}
	return true
}

// hasLiveDependent reports whether any object currently present in the store
// declares key as a dependency and is not itself being deleted. A deleting
// dependent is already tearing down and therefore must not block key's own
// removal (this is what makes a dependency chain delete leaf-first). This
// consults the source of truth, avoiding a transient gap in the graph's
// reverse index during event processing.
func (c *Controller) hasLiveDependent(ctx context.Context, key string) (bool, error) {
	objs, err := c.cfg.Store.List(ctx)
	if err != nil {
		return false, err
	}
	for _, o := range objs {
		if o.Key() == key || o.BeingDeleted() {
			continue
		}
		for _, ref := range dependency.NormalizeRefs(o.Namespace, o.Dependencies) {
			if ref == key {
				return true, nil
			}
		}
	}
	return false, nil
}

// recordIfTerminal promotes a queue terminal error into the controller's
// observable failure set with a distinguishable reason.
func (c *Controller) recordIfTerminal(key string, reason FailureReason, term error) {
	if term == nil {
		return
	}
	c.fail(key, reason, attemptsFrom(term), term)
}

// fail records a terminal conclusion (never a silent drop).
func (c *Controller) fail(key string, reason FailureReason, attempts int, err error) {
	c.mu.Lock()
	c.failures[key] = &TerminalFailure{Key: key, Reason: reason, Attempts: attempts, Err: err}
	c.mu.Unlock()
	c.log("TERMINAL key=%s reason=%s err=%v", key, reason, err)
}

// clearFailure forgets a terminal conclusion for key. It is invoked on every
// fresh trigger, so a missing dependency being created, a cycle edge being
// removed, or any new desired-state generation automatically revives a key
// that previously had a terminal conclusion.
func (c *Controller) clearFailure(key string) {
	c.mu.Lock()
	delete(c.failures, key)
	c.mu.Unlock()
}

// InFlight returns the number of reconciles currently executing.
func (c *Controller) InFlight() int { return int(c.inFlight.Load()) }

// Failures returns a snapshot of terminally failed keys.
func (c *Controller) Failures() map[string]error {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]error, len(c.failures))
	for k, v := range c.failures {
		out[k] = v
	}
	return out
}

// attemptsFrom extracts the attempt count from a queue ExhaustedError.
func attemptsFrom(err error) int {
	var ex *queue.ExhaustedError
	if errors.As(err, &ex) {
		return ex.Attempts
	}
	return 0
}
