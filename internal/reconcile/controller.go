package reconcile

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xingjianluo/reconcile-engine-go/internal/object"
	"github.com/xingjianluo/reconcile-engine-go/internal/queue"
	"github.com/xingjianluo/reconcile-engine-go/internal/store"
)

// Config configures a Controller.
type Config struct {
	Store         store.Store
	Queue         queue.Queue
	Reconciler    Reconciler
	Workers       int
	ShutdownGrace time.Duration
	Logger        func(format string, args ...any)
}

// Controller distributes store events to the queue and runs workers that
// process each resource's changes serially and in version order.
type Controller struct {
	cfg Config

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
	return &Controller{cfg: cfg, failures: make(map[string]error)}
}

// log prefixes lines so test/demo output is traceable.
func (c *Controller) log(format string, args ...any) {
	c.cfg.Logger("    [controller] "+format, args...)
}

// Run starts the event pump and workers, blocking until ctx is cancelled.
// On cancellation it stops pulling new events, lets the queue drain its
// ready work and tracks in-flight reconciles so they remain observable.
func (c *Controller) Run(ctx context.Context) error {
	watchCtx, cancelWatch := context.WithCancel(context.Background())
	defer cancelWatch()

	events, err := c.cfg.Store.Watch(watchCtx)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup

	// Event pump: store -> queue.
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.pump(ctx, events)
	}()

	// Initial list so objects created before Run are still reconciled.
	if objs, lerr := c.cfg.Store.List(ctx); lerr == nil {
		for _, o := range objs {
			c.enqueueObject(o)
		}
	}

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
	cancelWatch()

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

// pump forwards store events into the coalescing queue. Delete events carry
// no object; the key is still enqueued so a final reconcile can observe the
// tombstone/absence. Stale (older-version) events are dropped by the queue.
func (c *Controller) pump(ctx context.Context, events <-chan store.Event) {
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-events:
			if !ok {
				return
			}
			switch ev.Type {
			case store.EventUpsert:
				c.enqueueObject(ev.Object)
			case store.EventDelete:
				c.cfg.Queue.Add(ev.Key, queue.EnqueueOptions{})
			}
		}
	}
}

// enqueueObject enqueues an object with its observed resource version.
func (c *Controller) enqueueObject(o *object.Object) {
	if o == nil {
		return
	}
	c.cfg.Queue.Add(o.Key(), queue.EnqueueOptions{
		ResourceVersion: o.ResourceVersion,
		HasVersion:      true,
	})
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
