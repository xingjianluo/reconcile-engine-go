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
	// requests such as waiting for an external condition).
	RequeueWith(key string, delay time.Duration)
	// Fail marks the in-flight item as terminally failed with an explicit
	// reason and does not retry it.
	Fail(key string, reason error)
	// Len returns the number of keys waiting to be processed.
	Len() int
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
// waiting map (ready or delayed), the inFlight map (being processed), or the
// failures map (terminally failed).
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

	mu       sync.Mutex
	cond     *sync.Cond
	waiting  map[string]*item
	inFlight map[string]*item
	failures map[string]error

	// closed means Get should hand out remaining ready work then report done.
	closed bool
	// addCh gates producers when capacity is saturated.
	notFull *sync.Cond
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
		cfg:      cfg,
		waiting:  make(map[string]*item),
		inFlight: make(map[string]*item),
		failures: make(map[string]error),
	}
	q.cond = sync.NewCond(&q.mu)
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
	q.addLocked(key, time.Time{} /*ready now*/, opts, true)
}

// AddAfter schedules a key after delay.
func (q *BasicQueue) AddAfter(key string, delay time.Duration, opts EnqueueOptions) {
	q.addLocked(key, q.cfg.Clock().Add(delay), opts, true)
}

// addLocked enqueues or merges. blockCapacity=false is used internally for
// re-queues which never exceed capacity (the slot already exists).
func (q *BasicQueue) addLocked(key string, readyAt time.Time, opts EnqueueOptions, blockCapacity bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if blockCapacity {
		// Bound memory without dropping: wait while every slot is occupied.
		for !q.closed && len(q.waiting)+len(q.inFlight) >= q.cfg.Capacity {
			q.notFull.Wait()
		}
	}

	// Already processing: merge so Done re-runs with the newest version.
	if it, ok := q.inFlight[key]; ok {
		if it.newerWins(opts.ResourceVersion, opts.HasVersion) {
			it.dirty = true
		}
		return
	}

	if it, ok := q.waiting[key]; ok {
		if !it.newerWins(opts.ResourceVersion, opts.HasVersion) {
			return
		}
		// Advancing readiness earlier is allowed; never delay an earlier item.
		if readyAt.IsZero() || readyAt.Before(it.readyAt) {
			it.readyAt = readyAt
		}
		q.cond.Broadcast()
		return
	}

	// A genuinely new change revives a key that had terminally failed: the
	// old conclusion belongs to the previous generation of desired state.
	if _, failed := q.failures[key]; failed && blockCapacity {
		delete(q.failures, key)
	}

	if q.closed && blockCapacity {
		return // refuse brand-new work after shutdown
	}

	q.waiting[key] = &item{readyAt: readyAt, version: opts.ResourceVersion, hasVer: opts.HasVersion}
	q.cond.Broadcast()
}

// Get returns the next ready key, or reports shutdown when none remain.
func (q *BasicQueue) Get(ctx context.Context) (string, bool) {
	// Wake the waiters promptly if the caller gives up.
	stop := context.AfterFunc(ctx, func() {
		q.mu.Lock()
		q.cond.Broadcast()
		q.mu.Unlock()
	})
	defer stop()

	q.mu.Lock()
	defer q.mu.Unlock()

	for {
		if err := ctx.Err(); err != nil {
			return "", false
		}
		now := q.cfg.Clock()
		var wakeIn time.Duration
		for key, it := range q.waiting {
			if !it.readyAt.IsZero() && it.readyAt.After(now) {
				d := it.readyAt.Sub(now)
				if wakeIn == 0 || d < wakeIn {
					wakeIn = d
				}
				continue
			}
			q.inFlight[key] = it
			delete(q.waiting, key)
			return key, true
		}
		// Nothing ready.
		if q.closed && len(q.waiting) == 0 && len(q.inFlight) == 0 {
			return "", false
		}
		if wakeIn > 0 {
			timer := time.AfterFunc(wakeIn, func() {
				q.mu.Lock()
				q.cond.Broadcast()
				q.mu.Unlock()
			})
			q.cond.Wait()
			timer.Stop()
		} else {
			q.cond.Wait()
		}
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
		q.cond.Broadcast()
		return
	}
	it.dirty = false
	it.attempts = 0
	it.lastErr = nil
	it.readyAt = time.Time{}
	q.waiting[key] = it
	q.cond.Broadcast()
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
		q.cond.Broadcast()
		return term
	}
	it.readyAt = q.cfg.Clock().Add(q.cfg.Backoff.Delay(it.attempts))
	q.waiting[key] = it
	q.cond.Broadcast()
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
	q.waiting[key] = it
	q.cond.Broadcast()
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
	q.cond.Broadcast()
}

// Len reports waiting keys.
func (q *BasicQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.waiting)
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
	q.cond.Broadcast()
	q.notFull.Broadcast()
	q.mu.Unlock()

	stop := context.AfterFunc(ctx, func() {
		q.mu.Lock()
		q.cond.Broadcast()
		q.mu.Unlock()
	})
	defer stop()

	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.waiting) > 0 || len(q.inFlight) > 0 {
		if err := ctx.Err(); err != nil {
			return &ShutdownIncompleteError{
				Waiting: len(q.waiting), InFlight: len(q.inFlight),
			}
		}
		q.cond.Wait()
	}
	return nil
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
