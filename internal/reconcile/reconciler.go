// Package reconcile wires the store and queue into a controller that
// distributes resource changes to idempotent reconcile funcs, serializes
// changes per resource, and drives finalizer-based deletion.
package reconcile

import (
	"context"

	"github.com/xingjianluo/reconcile-engine-go/internal/object"
)

// Result tells the controller what to do after one reconcile invocation.
type Result struct {
	// Requeue asks for another run after the backoff delay.
	Requeue bool
}

// FailureReason classifies why a reconcile attempt failed, so exhausted
// retries yield a distinguishable conclusion rather than a silent drop.
type FailureReason string

const (
	// ReasonReconcileError is an ordinary reconcile-func error.
	ReasonReconcileError FailureReason = "ReconcileError"
	// ReasonRetryExhausted means max attempts were reached.
	ReasonRetryExhausted FailureReason = "RetryExhausted"
	// ReasonConflictLoop means the object kept changing under us.
	ReasonConflictLoop FailureReason = "ConflictLoop"
	// ReasonDependencyMissing means the object references a dependency that
	// does not exist. It is a distinguishable terminal conclusion for the
	// current generation; creating the dependency revives the object.
	ReasonDependencyMissing FailureReason = "DependencyMissing"
	// ReasonDependencyCycle means the object's declarations participate in a
	// dependency cycle that can never become ready under the current graph.
	ReasonDependencyCycle FailureReason = "DependencyCycle"
)

// GuardFinalizer is the engine-managed finalizer that enforces reverse
// deletion order: it is added to every live object that currently has
// dependents, so such an object cannot physically disappear until every
// dependent has itself physically disappeared. A dependent merely marked for
// deletion (still present in the store) keeps blocking; only its actual
// removal unblocks this object. The guard is removed automatically once no
// dependent remains, without app reconciler cooperation.
const GuardFinalizer = "reconcile-engine.io/dependency-guard"

// TerminalFailure records the distinguishable outcome of an exhausted key.
type TerminalFailure struct {
	Key      string
	Reason   FailureReason
	Attempts int
	Err      error
}

func (f *TerminalFailure) Error() string {
	return "reconcile: terminal failure for " + f.Key +
		" reason=" + string(f.Reason) +
		" attempts=" + itoa(f.Attempts) +
		": " + safeErr(f.Err)
}

// Unwrap exposes the underlying error.
func (f *TerminalFailure) Unwrap() error { return f.Err }

// Reconciler performs one idempotent convergence attempt for an object.
type Reconciler interface {
	Reconcile(ctx context.Context, obj *object.Object) (Result, error)
}

// ReconcilerFunc adapts a function to the Reconciler interface.
type ReconcilerFunc func(ctx context.Context, obj *object.Object) (Result, error)

// Reconcile calls f.
func (f ReconcilerFunc) Reconcile(ctx context.Context, obj *object.Object) (Result, error) {
	return f(ctx, obj)
}

// safeErr renders nil safely.
func safeErr(err error) string {
	if err == nil {
		return "<nil>"
	}
	return err.Error()
}

// itoa renders a small non-negative counter without importing strconv here.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
