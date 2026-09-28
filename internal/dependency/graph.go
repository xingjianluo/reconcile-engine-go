package dependency

import (
	"sort"
	"sync"

	"github.com/xingjianluo/reconcile-engine-go/internal/object"
)

// node is the graph's per-key state: the sorted/deduplicated set of
// dependencies it declares plus its current readiness.
type node struct {
	deps     []string
	ready    bool
	deleting bool
}

// Graph is the concurrency-safe directed dependency graph. Edges run from a
// dependent to each dependency it declares ("dependent -> dependency"). A
// reverse index ("dependency -> dependents") drives change propagation.
//
// The graph is purely derived from observed object state; its memory is
// bounded by the number of distinct declared keys, and the queue's bound
// caps the fan-out of change propagation.
type Graph struct {
	mu sync.RWMutex
	// nodes holds live objects; absence means a referenced dependency is
	// missing (a distinguishable failure, not an infinite wait).
	nodes map[string]*node
	// rev is the reverse index depKey -> set of dependent keys. Entries are
	// removed only when the *dependent* changes its declaration or is deleted;
	// deleting the dependency itself keeps the entry so recreation propagates.
	rev map[string]map[string]struct{}
}

// NewGraph constructs an empty graph.
func NewGraph() *Graph {
	return &Graph{
		nodes: make(map[string]*node),
		rev:   make(map[string]map[string]struct{}),
	}
}

// DefaultReady is the standard readiness judgment: an object unblocks its
// dependents only when its reconciler has set Ready=true and it is not being
// deleted. Controllers may supply a different predicate via Config.IsReady.
func DefaultReady(o *object.Object) bool {
	return o != nil && o.Ready && !o.BeingDeleted()
}

// NormalizeRefs expands namespace-less references into the owner's namespace,
// then deduplicates and sorts them, giving every graph operation a stable
// input order (deterministic errors, no duplicate fan-out).
func NormalizeRefs(ownerNamespace string, refs []object.DependencyRef) []string {
	if len(refs) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(refs))
	out := make([]string, 0, len(refs))
	for _, r := range refs {
		ns := r.Namespace
		if ns == "" {
			ns = ownerNamespace
		}
		key := object.MakeKey(ns, r.Name)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

// Upsert inserts or replaces a node's declared edges and readiness. The
// reverse index is adjusted for exactly the edges added/removed, so repeated
// upserts of unchanged declarations are idempotent. It returns the sorted
// dependency keys that were newly referenced by this upsert (the upstream
// targets), so the caller can trigger them to attach/maintain reverse-deletion
// guards that may have been absent before the edge existed.
func (g *Graph) Upsert(key string, deps []string, ready, deleting bool) []string {
	g.mu.Lock()
	defer g.mu.Unlock()

	n := g.nodes[key]
	if n == nil {
		n = &node{}
		g.nodes[key] = n
	}
	old := n.deps
	n.deps = append([]string(nil), deps...)
	n.ready = ready
	n.deleting = deleting

	added, removed := diffEdges(old, n.deps)
	for _, d := range added {
		set := g.rev[d]
		if set == nil {
			set = make(map[string]struct{})
			g.rev[d] = set
		}
		set[key] = struct{}{}
	}
	for _, d := range removed {
		if set, ok := g.rev[d]; ok {
			delete(set, key)
			if len(set) == 0 {
				delete(g.rev, d)
			}
		}
	}
	return append([]string(nil), added...)
}

// Replace rebuilds the whole graph from a snapshot. It is used once at
// startup (Watch established before the initial List) so the first reconcile
// gates against the complete graph rather than trickling declarations.
func (g *Graph) Replace(entries []SnapshotEntry) {
	nodes := make(map[string]*node, len(entries))
	rev := make(map[string]map[string]struct{})
	for _, e := range entries {
		deps := append([]string(nil), e.Deps...)
		nodes[e.Key] = &node{deps: deps, ready: e.Ready, deleting: e.Deleting}
		for _, d := range deps {
			set := rev[d]
			if set == nil {
				set = make(map[string]struct{})
				rev[d] = set
			}
			set[e.Key] = struct{}{}
		}
	}
	g.mu.Lock()
	g.nodes = nodes
	g.rev = rev
	g.mu.Unlock()
}

// SnapshotEntry is one node's full state used by Replace.
type SnapshotEntry struct {
	Key      string
	Deps     []string
	Ready    bool
	Deleting bool
}

// Delete removes key's node and its outgoing edges and returns the sorted
// keys that declared key as a dependency, so the caller can re-enqueue them
// (they now observe a missing dependency). Incoming references are kept in
// the reverse index: they are still declared and must propagate again if key
// is ever recreated.
func (g *Graph) Delete(key string) []string {
	g.mu.Lock()
	defer g.mu.Unlock()

	n := g.nodes[key]
	if n != nil {
		for _, d := range n.deps {
			if set, ok := g.rev[d]; ok {
				delete(set, key)
				if len(set) == 0 {
					delete(g.rev, d)
				}
			}
		}
		delete(g.nodes, key)
	}

	dependents := g.sortedDependentsLocked(key)
	return dependents
}

// Dependents returns a sorted snapshot of keys that declare dep as a
// dependency.
func (g *Graph) Dependents(dep string) []string {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.sortedDependentsLocked(dep)
}

func (g *Graph) sortedDependentsLocked(dep string) []string {
	set := g.rev[dep]
	if len(set) == 0 {
		return nil
	}
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Blockers partitions key's declared dependencies into missing (no live node)
// and notReady (live but deleting or not yet ready). Both slices are sorted
// and deduplicated. A key with no blockers is free to reconcile.
func (g *Graph) Blockers(key string) (missing, notReady []string) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	n := g.nodes[key]
	if n == nil {
		return nil, nil
	}
	for _, d := range n.deps {
		dn, ok := g.nodes[d]
		if !ok {
			missing = append(missing, d)
			continue
		}
		if dn.deleting || !dn.ready {
			notReady = append(notReady, d)
		}
	}
	return missing, notReady
}

// Cycle searches for a directed cycle reachable from key through edges to
// existing nodes. It returns the cycle as a slice starting and ending at key
// (e.g. [a, b, a]); nil means no cycle. Edges to missing nodes cannot be part
// of a loop and are not followed.
func (g *Graph) Cycle(key string) []string {
	g.mu.RLock()
	defer g.mu.RUnlock()
	if _, ok := g.nodes[key]; !ok {
		return nil
	}

	const (
		white = 0
		gray  = 1
		black = 2
	)
	color := map[string]int{key: gray}
	stack := []string{key}

	var dfs func(string) []string
	dfs = func(cur string) []string {
		n := g.nodes[cur]
		if n == nil {
			return nil
		}
		for _, d := range n.deps {
			if _, ok := g.nodes[d]; !ok {
				continue // missing dependency cannot close a loop
			}
			switch color[d] {
			case gray:
				// Back edge: extract the loop from where d first appears.
				loop := append([]string(nil), stack...)
				for i := 0; i < len(loop); i++ {
					if loop[i] == d {
						loop = loop[i:]
						break
					}
				}
				loop = append(loop, d)
				return loop
			case white:
				color[d] = gray
				stack = append(stack, d)
				if found := dfs(d); found != nil {
					return found
				}
				stack = stack[:len(stack)-1]
				color[d] = black
			}
		}
		color[cur] = black
		return nil
	}
	return dfs(key)
}

// diffEdges returns the sorted dependency keys present in next but not prev
// (added) and prev but not next (removed). Both inputs are sorted/unique.
func diffEdges(prev, next []string) (added, removed []string) {
	i, j := 0, 0
	for i < len(prev) || j < len(next) {
		switch {
		case i >= len(prev):
			added = append(added, next[j])
			j++
		case j >= len(next):
			removed = append(removed, prev[i])
			i++
		case prev[i] < next[j]:
			removed = append(removed, prev[i])
			i++
		case prev[i] > next[j]:
			added = append(added, next[j])
			j++
		default:
			i++
			j++
		}
	}
	return added, removed
}
