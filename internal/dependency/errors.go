// Package dependency maintains the dependency graph declared by objects and
// answers the two ordering questions the controller needs: (1) which not-yet-
// ready/missing dependencies block a key, and (2) whether a key participates
// in a dependency cycle. Both abnormal conclusions are reported as distinct,
// observable error types rather than silent retries.
package dependency

import "strings"

// MissingError is the distinguishable conclusion for an object that references
// a dependency which does not exist in the graph. It is a terminal conclusion
// for the current generation of state: it is retried (and succeeds) once the
// referenced object is created, so it never loops forever and never vanishes
// silently.
type MissingError struct {
	// Key is the dependent that declared the reference.
	Key string
	// Missing lists the unresolved dependency keys (deduplicated, sorted).
	Missing []string
}

// Error renders the missing-dependency conclusion.
func (e *MissingError) Error() string {
	return "dependency: " + e.Key + " references missing dependency/ies: " + strings.Join(e.Missing, ", ")
}

// CycleError is the distinguishable conclusion for an object whose declared
// dependencies form a directed cycle that reaches itself. Such an object can
// never become unblocked under the current declarations, so the engine records
// a terminal conclusion instead of waiting forever. Breaking the edge
// re-enqueues the affected objects automatically.
type CycleError struct {
	// Key is the member the cycle was reported for.
	Key string
	// Cycle lists the keys along the loop, starting and ending at Key.
	Cycle []string
}

// Error renders the cycle conclusion.
func (e *CycleError) Error() string {
	return "dependency: cycle detected involving " + e.Key + ": " + strings.Join(e.Cycle, " -> ")
}
