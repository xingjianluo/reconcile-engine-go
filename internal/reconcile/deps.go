package reconcile

import (
	"context"
	"fmt"
	"strings"

	"github.com/xingjianluo/reconcile-engine-go/internal/object"
	"github.com/xingjianluo/reconcile-engine-go/internal/store"
)

// DependencyMissingError is the distinguishable conclusion for an object that
// declares a dependency no object in the store provides. It is terminal (not
// retried forever): the failure is recorded and observable, and a later
// creation of the missing key re-triggers the dependent through propagation.
type DependencyMissingError struct {
	Key        string
	Dependency string
}

func (e *DependencyMissingError) Error() string {
	return fmt.Sprintf("reconcile: %s depends on %s, which does not exist", e.Key, e.Dependency)
}

// DependencyCycleError is the distinguishable conclusion for an object whose
// dependency graph contains a cycle (including a self-dependency). Path holds
// the cycle in declaration order, e.g. [a b a]. It is terminal: the engine
// fails fast instead of deadlocking or retrying forever, and editing any
// object on the path re-triggers evaluation through propagation.
type DependencyCycleError struct {
	Key  string
	Path []string
}

func (e *DependencyCycleError) Error() string {
	return fmt.Sprintf("reconcile: dependency cycle reachable from %s: %s",
		e.Key, strings.Join(e.Path, " -> "))
}

// ReadyFunc reports whether a dependency object counts as ready. obj is
// guaranteed non-nil and not being deleted when called.
type ReadyFunc func(obj *object.Object) bool

// DefaultReady is the built-in readiness rule: the dependency has converged,
// i.e. its observed Status deep-equals its desired Spec. Objects with no spec
// and no status are trivially ready.
func DefaultReady(obj *object.Object) bool {
	if len(obj.Spec) != len(obj.Status) {
		return false
	}
	for k, v := range obj.Spec {
		if obj.Status[k] != v {
			return false
		}
	}
	return true
}

// depVerdict is the outcome of evaluating one object's dependencies.
type depVerdict int

const (
	// depOK means every dependency exists and is ready; proceed to reconcile.
	depOK depVerdict = iota
	// depWait means at least one dependency exists but is not ready yet;
	// wait quietly and re-check later (not an error, not an attempt).
	depWait
	// depMissing means a declared dependency does not exist (terminal).
	depMissing
	// depCycle means a cycle is reachable from this object (terminal).
	depCycle
)

// evaluateDeps checks obj's declared dependencies against the store.
// Order matters: cycles are detected first (a self-dependency would
// otherwise look like "not ready" and wait forever), then existence, then
// readiness. The returned error is set for the terminal verdicts only.
func evaluateDeps(ctx context.Context, s store.Store, obj *object.Object, ready ReadyFunc) (depVerdict, error) {
	if len(obj.DependsOn) == 0 {
		return depOK, nil
	}
	if path := findCycle(ctx, s, obj.Key()); path != nil {
		return depCycle, &DependencyCycleError{Key: obj.Key(), Path: path}
	}
	for _, dep := range obj.DependsOn {
		depObj, err := s.Get(ctx, dep)
		if err != nil {
			return depMissing, &DependencyMissingError{Key: obj.Key(), Dependency: dep}
		}
		if depObj.BeingDeleted() {
			// A dependency that is going away is not ready; wait quietly.
			// Its physical removal re-triggers this object via propagation,
			// at which point the missing-dependency conclusion applies.
			return depWait, nil
		}
		if !ready(depObj) {
			// Exists but not converged yet: wait quietly and re-check.
			return depWait, nil
		}
	}
	return depOK, nil
}

// findCycle reports a dependency cycle reachable from start, following
// DependsOn edges through the store. It returns the cycle path (first node
// repeated at the end) or nil. Missing nodes terminate their branch: a
// missing dependency is a different, separately reported conclusion.
func findCycle(ctx context.Context, s store.Store, start string) []string {
	const (
		white = iota // unvisited
		gray         // on the current DFS path
		black        // fully explored, acyclic from here
	)
	color := map[string]int{}
	var stack []string

	var visit func(key string) []string
	visit = func(key string) []string {
		switch color[key] {
		case gray:
			// Found a back edge: extract the cycle from the stack.
			for i, k := range stack {
				if k == key {
					return append(append([]string(nil), stack[i:]...), key)
				}
			}
			return []string{key, key}
		case black:
			return nil
		}
		color[key] = gray
		stack = append(stack, key)
		obj, err := s.Get(ctx, key)
		if err == nil {
			for _, dep := range obj.DependsOn {
				if cyc := visit(dep); cyc != nil {
					return cyc
				}
			}
		}
		stack = stack[:len(stack)-1]
		color[key] = black
		return nil
	}
	return visit(start)
}

// liveDependents returns the keys of existing objects that declare a
// dependency on key. It scans the store (the source of truth) so the answer
// is always current, regardless of event delivery order.
func liveDependents(ctx context.Context, s store.Store, key string) ([]string, error) {
	objs, err := s.List(ctx)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, o := range objs {
		if o.Key() != key && o.DependsOnKey(key) {
			out = append(out, o.Key())
		}
	}
	return out, nil
}
