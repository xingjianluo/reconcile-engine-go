package queue

import (
	"context"
	"errors"
	"sync"
	"time"
)

// EnqueueOptions carries an optional resource version used for coalescing.
type EnqueueOptions struct {
	// ResourceVersion is the latest version the producer observed. An enqueue
	// carrying an older version than one already observed is merged away so
	// stale events never trigger work on obsolete data.
	ResourceVersion int64
	HasVersion      bool
}

// Queue is the work queue contract used by the controller.
type Queue interface {
	// Add coalesces a key: repeated adds before the key is processed result
	// in a single queued item.
	Add(key string, opts EnqueueOptions)
	// TryAdd is the non-blocking form of Add: it coalesces when a slot is
	// available and reports false (without enqueueing or dropping) when the
	// bounded set is saturated. Callers that must not stall while waiting for
	// a slot use this and retry on the next opportunity, so no item is lost.
	TryAdd(key string, opts EnqueueOptions) bool
	// AddAfter schedules a key for (re)processing no earlier than delay.
	AddAfter(key string, delay time.Duration, opts EnqueueOptions)
	// Get blocks until an item is ready or ctx is cancelled. The returned
	// item must be reported via Done, Requeue or Fail. ok is false on
	// shutdown with no remaining work.
	Get(ctx context.Context) (key string, ok bool)
	// Done acknowledges successful processing of the in-flight item. If a
	// newer Add merged in while it was processing, the key is re-queued.
	Done(key string)
	// Requeue schedules another attempt of the in-flight item using the
	// configured backoff based on its attempt count. It returns nil while a
	// retry was scheduled. When attempts are exhausted it does NOT retry: it
	// records and returns a distinguishable *ExhaustedError instead of
	// silently dropping the key.
	Requeue(key string, cause error) error
	// RequeueWith returns an in-flight item for another attempt after delay,
	// without counting it as a failed attempt (used for explicit requeue
	// requests such as waiting for a dependency to become ready). Delayed
	// items park in a scheduled set that does NOT consume bounded runnable
	// capacity, so work which merely waits on a condition cannot starve work
	// that is ready to run (see Runnable).
	RequeueWith(key string, delay time.Duration)
	// Fail marks the in-flight item as terminally failed with an explicit
	// reason and does not retry it.
	Fail(key string, reason error)
	// Len returns the total number of keys awaiting processing: runnable
	// items plus delayed items parked in the scheduled set.
	Len() int
	// Runnable returns the number of immediately dispatchable items; together
	// with InFlight it is bounded by Capacity.
	Runnable() int
	// WorkSet returns a single atomic snapshot of Runnable()+InFlight().
	WorkSet() int
	// InFlight returns the number of keys currently being processed.
	InFlight() int
	// Shutdown stops accepting new work and blocks while in-flight and
	// ready-but-unprocessed keys drain, up to ctx; it returns an error if
	// work remains when the deadline is reached.
	Shutdown(ctx context.Context) error
	// Failures returns a snapshot of terminally failed keys and reasons.
	Failures() map[string]error
}

// defaultCapacity bounds the number of distinct tracked keys. It is a safety
// valve, not a lossy drop: Add blocks until a slot frees rather than dropping.
const defaultCapacity = 10000

// Config tunes a BasicQueue.
type Config struct {
	Backoff       Backoff
	MaxAttempts   int
	Capacity      int
	ShutdownGrace time.Duration
	Clock         func() time.Time
}

// item is the per-key bookkeeping. A key exists in exactly one of: the
// waiting map (runnable NOW), the scheduled map (delayed, e.g. backoff or a
// quiet dependency wait), the inFlight map (being processed), or the failures
// map (terminally failed).
type item struct {
	attempts int
	readyAt  time.Time
	version  int64
	hasVer   bool
	lastErr  error
	// dirty marks that a newer Add merged in while the key was in flight.
	dirty bool
}

// BasicQueue is the reference bounded, coalescing, per-key-serial queue.
type BasicQueue struct {
	cfg Config

	mu sync.Mutex
	// waiting holds RUNNABLE items (readyAt zero/past). Only this set, along
	// with inFlight, counts against Capacity: a dependency/backoff wait that
	// cannot yet make progress parks in scheduled instead, so a queue full of
	// waiters can never starve an item that is actually runnable.
	waiting   map[string]*item
	scheduled map[string]*item
	inFlight  map[string]*item
	failures  map[string]error

	// closed means Get should hand out remaining ready work then report done.
	closed bool

	// waiters is the set of per-Get one-shot wake channels currently parked.
	// broadcastLocked closes every registered channel; a waiter registers
	// while holding mu in the same critical section in which it observed "no
	// ready item", so a broadcast produced by a later state change is
	// guaranteed to close that exact channel. There is no lost-wakeup window.
	waiters map[chan struct{}]struct{}
	// notFull gates producers (Add) when capacity is saturated.
	notFull *sync.Cond
}

// broadcastLocked wakes every parked Get/Shutdown waiter. Callers MUST hold mu.
func (q *BasicQueue) broadcastLocked() {
	for ch := range q.waiters {
		close(ch)
		delete(q.waiters, ch)
	}
}

// New constructs a BasicQueue with the given configuration.
func New(cfg Config) *BasicQueue {
	if cfg.Backoff == nil {
		cfg.Backoff = NewExponentialBackoff(10*time.Millisecond, time.Second)
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 5
	}
	if cfg.Capacity <= 0 {
		cfg.Capacity = defaultCapacity
	}
	if cfg.Clock == nil {
		cfg.Clock = time.Now
	}
	q := &BasicQueue{
		cfg:       cfg,
		waiting:   make(map[string]*item),
		scheduled: make(map[string]*item),
		inFlight:  make(map[string]*item),
		failures:  make(map[string]error),
		waiters:   make(map[chan struct{}]struct{}),
	}
	q.notFull = sync.NewCond(&q.mu)
	return q
}

// newerWins merges an incoming version into it. Returns false when the
// incoming event is strictly older and must be discarded.
func (it *item) newerWins(ver int64, hasVer bool) bool {
	if !hasVer {
		return true // unversioned triggers (e.g. retry) are always accepted
	}
	if !it.hasVer {
		it.version, it.hasVer = ver, true
		return true
	}
	if ver < it.version {
		return false // stale event: merge away
	}
	it.version = ver
	return true
}

// Add coalesces a key for immediate processing.
func (q *BasicQueue) Add(key string, opts EnqueueOptions) {
	q.addLocked(key, time.Time{} /*ready now*/, opts, true /*block*/)
}

// TryAdd coalesces a key for immediate processing without blocking. It
// returns false (and changes nothing) only when the bounded tracked set is
// saturated; the caller retries later so no item is dropped.
func (q *BasicQueue) TryAdd(key string, opts EnqueueOptions) bool {
	return q.addLocked(key, time.Time{}, opts, false /*do not block*/)
}

// AddAfter schedules a key after delay.
func (q *BasicQueue) AddAfter(key string, delay time.Duration, opts EnqueueOptions) {
	q.addLocked(key, q.cfg.Clock().Add(delay), opts, true)
}

// addLocked enqueues or merges a key. Immediate items (readyAt zero) occupy a
// bounded runnable slot and block (Add) or reject (TryAdd) when saturated;
// delayed items park in the scheduled set, which does not consume runnable
// capacity, so runnable progress cannot be starved by items that are merely
// waiting.
func (q *BasicQueue) addLocked(key string, readyAt time.Time, opts EnqueueOptions, block bool) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Merge into in-flight regardless of timing.
	if it, ok := q.inFlight[key]; ok {
		if it.newerWins(opts.ResourceVersion, opts.HasVersion) {
			it.dirty = true
		}
		return true
	}

	// Merge into a scheduled (waiting) item. An immediate enqueue promotes it
	// to runnable (subject to capacity below); a delayed enqueue only updates
	// its deadline/version.
	if it, ok := q.scheduled[key]; ok {
		if !it.newerWins(opts.ResourceVersion, opts.HasVersion) {
			return true
		}
		if readyAt.IsZero() {
			if !q.acquireSlotLocked(key, block, false) {
				return false
			}
			delete(q.scheduled, key)
			it.readyAt = time.Time{}
			q.waiting[key] = it
			q.broadcastLocked()
			return true
		}
		// Never move readiness later than currently scheduled.
		if readyAt.Before(it.readyAt) {
			it.readyAt = readyAt
		}
		q.broadcastLocked()
		return true
	}

	if it, ok := q.waiting[key]; ok {
		if !it.newerWins(opts.ResourceVersion, opts.HasVersion) {
			return true
		}
		// Already runnable: advancing earlier is fine; do not push it later.
		if !readyAt.IsZero() && readyAt.After(it.readyAt) && !it.readyAt.IsZero() {
			// Keep the earlier (runnable) slot.
			return true
		}
		if !readyAt.IsZero() && it.readyAt.IsZero() {
			// Move a runnable item to scheduled only if explicitly delayed and
			// it had no immediate slot requirement; keep it runnable otherwise.
			return true
		}
		if readyAt.IsZero() || readyAt.Before(it.readyAt) {
			it.readyAt = readyAt
		}
		q.broadcastLocked()
		return true
	}

	// A genuinely new change revives a terminally failed key.
	if _, failed := q.failures[key]; failed {
		if q.closed && block {
			return false
		}
		delete(q.failures, key)
	} else if q.closed && block {
		return false
	}

	it := &item{readyAt: readyAt, version: opts.ResourceVersion, hasVer: opts.HasVersion}
	if readyAt.IsZero() {
		if !q.acquireSlotLocked(key, block, false) {
			return false
		}
		q.waiting[key] = it
	} else {
		q.scheduled[key] = it
	}
	q.broadcastLocked()
	return true
}

// acquireSlotLocked waits for (or, when block=false, checks for) a free
// bounded runnable slot. scheduled keys do NOT occupy a runnable slot, so
// promoting one requires a genuinely free slot; only a key already runnable
// or in-flight merges without a new slot. Callers MUST hold mu.
func (q *BasicQueue) acquireSlotLocked(key string, block bool, alreadyRunnable bool) bool {
	for !q.closed && len(q.waiting)+len(q.inFlight) >= q.cfg.Capacity {
		// Only a merge into a slot that is already part of the bounded work
		// set can proceed without a free slot.
		if alreadyRunnable {
			if _, ok := q.waiting[key]; ok {
				return true
			}
			if _, ok := q.inFlight[key]; ok {
				return true
			}
		}
		if !block {
			return false
		}
		q.notFull.Wait()
	}
	return !q.closed || q.waiting[key] != nil || q.inFlight[key] != nil
}

// Get returns the next ready key, or reports shutdown when none remain.
//
// Wakeup is race-free by construction: while holding mu the worker takes a
// ready item; otherwise it registers its own one-shot channel in waiters in
// the SAME critical section as the "nothing ready" observation, then releases
// mu and blocks on that channel. Every later state change closes all channels
// registered at that moment, so a change arriving just after the check cannot
// be missed — it necessarily closes the worker's channel. Spurious wakes just
// loop and re-register.
func (q *BasicQueue) Get(ctx context.Context) (string, bool) {
	stop := context.AfterFunc(ctx, func() {
		q.mu.Lock()
		q.broadcastLocked()
		q.mu.Unlock()
	})
	defer stop()

	for {
		q.mu.Lock()
		if err := ctx.Err(); err != nil {
			q.mu.Unlock()
			return "", false
		}
		now := q.cfg.Clock()

		// 1) Any runnable item is handed out immediately.
		if len(q.waiting) > 0 {
			for key, it := range q.waiting {
				delete(q.waiting, key)
				q.inFlight[key] = it
				q.mu.Unlock()
				return key, true
			}
		}

		// 2) Promote scheduled items whose deadline elapsed, provided a
		//    bounded runnable slot is free (one is being consumed now).
		var soonest time.Time
		for key, it := range q.scheduled {
			if it.readyAt.After(now) {
				if soonest.IsZero() || it.readyAt.Before(soonest) {
					soonest = it.readyAt
				}
				continue
			}
			if len(q.waiting)+len(q.inFlight) >= q.cfg.Capacity {
				// Runnable slots saturated; re-check when one frees (Done /
				// Requeue / Fail all broadcast). Treat the due item as
				// immediate: wake when capacity changes.
				soonest = now
				break
			}
			delete(q.scheduled, key)
			it.readyAt = time.Time{}
			q.inFlight[key] = it
			q.mu.Unlock()
			return key, true
		}

		if q.closed && len(q.waiting) == 0 && len(q.scheduled) == 0 && len(q.inFlight) == 0 {
			q.mu.Unlock()
			return "", false
		}

		// Register before releasing mu, closing the check-to-sleep gap.
		wake := make(chan struct{})
		q.waiters[wake] = struct{}{}
		q.mu.Unlock()

		// Arm a timer for the nearest future deadline. Its only job is to
		// wake us; readiness is always re-checked under the lock afterwards.
		var timer *time.Timer
		if !soonest.IsZero() {
			d := soonest.Sub(now)
			if d < 0 {
				d = 0
			}
			timer = time.AfterFunc(d, func() {
				q.mu.Lock()
				q.broadcastLocked()
				q.mu.Unlock()
			})
		}
		select {
		case <-ctx.Done():
		case <-wake:
		}
		if timer != nil {
			timer.Stop()
		}
		q.mu.Lock()
		delete(q.waiters, wake)
		q.mu.Unlock()
	}
}

// Done acknowledges an in-flight key. A merged newer version re-queues it
// immediately so no change is lost; otherwise the key is fully retired.
func (q *BasicQueue) Done(key string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	it, ok := q.inFlight[key]
	if !ok {
		return
	}
	delete(q.inFlight, key)
	q.notFull.Broadcast()

	if !it.dirty {
		// Converged and no newer change observed: retire the key.
		it.attempts = 0
		it.lastErr = nil
		q.broadcastLocked()
		return
	}
	it.dirty = false
	it.attempts = 0
	it.lastErr = nil
	it.readyAt = time.Time{}
	q.waiting[key] = it
	q.broadcastLocked()
}

// Requeue records a failed attempt and either backs off or fails terminally.
// It returns the terminal *ExhaustedError when retries are spent.
func (q *BasicQueue) Requeue(key string, cause error) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	it, ok := q.inFlight[key]
	if !ok {
		return nil
	}
	delete(q.inFlight, key)
	q.notFull.Broadcast()

	it.attempts++
	it.lastErr = cause

	if it.attempts >= q.cfg.MaxAttempts {
		term := &ExhaustedError{
			Key: key, Attempts: it.attempts, LastErr: cause,
		}
		q.failures[key] = term
		q.broadcastLocked()
		return term
	}
	it.readyAt = q.cfg.Clock().Add(q.cfg.Backoff.Delay(it.attempts))
	q.scheduled[key] = it
	q.broadcastLocked()
	return nil
}

// RequeueWith puts an in-flight item back after delay without incrementing
// the failed-attempt counter, preserving any merged newer version.
func (q *BasicQueue) RequeueWith(key string, delay time.Duration) {
	q.mu.Lock()
	defer q.mu.Unlock()
	it, ok := q.inFlight[key]
	if !ok {
		return
	}
	delete(q.inFlight, key)
	q.notFull.Broadcast()
	it.readyAt = q.cfg.Clock().Add(delay)
	q.scheduled[key] = it
	q.broadcastLocked()
}

// Fail records an explicit terminal failure for an in-flight key.
func (q *BasicQueue) Fail(key string, reason error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, ok := q.inFlight[key]; !ok {
		return
	}
	delete(q.inFlight, key)
	q.notFull.Broadcast()
	if reason == nil {
		reason = errors.New("unspecified failure")
	}
	q.failures[key] = reason
	q.broadcastLocked()
}

// Len reports the number of keys awaiting processing: runnable items plus
// delayed items parked in the scheduled set.
func (q *BasicQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.waiting) + len(q.scheduled)
}

// Runnable reports keys immediately dispatchable (the bounded work set).
func (q *BasicQueue) Runnable() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.waiting)
}

// WorkSet returns the bounded work-set size (runnable + in-flight) as a single
// atomic snapshot, so an external observer never reads a torn value.
func (q *BasicQueue) WorkSet() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.waiting) + len(q.inFlight)
}

// InFlight reports keys being processed.
func (q *BasicQueue) InFlight() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.inFlight)
}

// Failures snapshots terminal failures.
func (q *BasicQueue) Failures() map[string]error {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make(map[string]error, len(q.failures))
	for k, v := range q.failures {
		out[k] = v
	}
	return out
}

// Shutdown stops accepting new work and drains until the tracked sets empty
// or ctx expires. In-flight keys remain observable via InFlight meanwhile.
func (q *BasicQueue) Shutdown(ctx context.Context) error {
	q.mu.Lock()
	q.closed = true
	q.broadcastLocked()
	q.notFull.Broadcast()
	q.mu.Unlock()

	stop := context.AfterFunc(ctx, func() {
		q.mu.Lock()
		q.broadcastLocked()
		q.mu.Unlock()
	})
	defer stop()

	for {
		q.mu.Lock()
		if len(q.waiting) == 0 && len(q.scheduled) == 0 && len(q.inFlight) == 0 {
			q.mu.Unlock()
			return nil
		}
		if err := ctx.Err(); err != nil {
			err = &ShutdownIncompleteError{
				Waiting: len(q.waiting) + len(q.scheduled), InFlight: len(q.inFlight),
			}
			q.mu.Unlock()
			return err
		}
		wake := make(chan struct{})
		q.waiters[wake] = struct{}{}
		q.mu.Unlock()
		<-wake
	}
}

// ExhaustedError is the distinguishable conclusion for a key whose retries
// ran out. Callers detect it (and the attempt count/last error) rather than
// observing a silent drop.
type ExhaustedError struct {
	Key      string
	Attempts int
	LastErr  error
}

func (e *ExhaustedError) Error() string {
	return "queue: retry exhausted for " + e.Key +
		" after " + itoa(e.Attempts) + " attempts: " + safeErr(e.LastErr)
}

// Unwrap exposes the underlying last error.
func (e *ExhaustedError) Unwrap() error { return e.LastErr }

// ShutdownIncompleteError reports work still present when grace expired.
type ShutdownIncompleteError struct {
	Waiting  int
	InFlight int
}

func (e *ShutdownIncompleteError) Error() string {
	return "queue: shutdown grace expired with " +
		itoa(e.Waiting) + " waiting and " + itoa(e.InFlight) + " in flight"
}

func safeErr(err error) string {
	if err == nil {
		return "<nil>"
	}
	return err.Error()
}

// itoa avoids pulling in strconv solely for two small counters.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}
