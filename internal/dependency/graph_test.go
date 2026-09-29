package dependency

import (
	"errors"
	"testing"

	"github.com/xingjianluo/reconcile-engine-go/internal/object"
)

func TestNormalizeRefs(t *testing.T) {
	got := NormalizeRefs("ns", []object.DependencyRef{
		{Name: "b"},
		{Name: "a"},
		{Name: "b"},                 // duplicate
		{Namespace: "x", Name: "c"}, // explicit ns
	})
	want := []string{"ns/a", "ns/b", "x/c"}
	if len(got) != len(want) {
		t.Fatalf("got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v want %v", got, want)
		}
	}
	t.Logf("INPUT refs with same-ns + dup | JUDGE: normalized=%v", got)
}

func TestBlockersMissingAndNotReady(t *testing.T) {
	g := NewGraph()
	// b exists but not ready; c exists and ready; d missing.
	g.Upsert("ns/b", nil, false, false)
	g.Upsert("ns/c", nil, true, false)
	g.Upsert("ns/a", []string{"ns/b", "ns/c", "ns/d"}, false, false)

	missing, notReady := g.Blockers("ns/a")
	if len(missing) != 1 || missing[0] != "ns/d" {
		t.Fatalf("missing=%v want [ns/d]", missing)
	}
	if len(notReady) != 1 || notReady[0] != "ns/b" {
		t.Fatalf("notReady=%v want [ns/b]", notReady)
	}
	t.Logf("INPUT a->b(not ready),c(ready),d(missing) | JUDGE: missing=%v notReady=%v", missing, notReady)

	// b becomes ready -> only the missing dependency remains.
	g.Upsert("ns/b", nil, true, false)
	missing, notReady = g.Blockers("ns/a")
	if len(missing) != 1 || len(notReady) != 0 {
		t.Fatalf("after b ready: missing=%v notReady=%v", missing, notReady)
	}
	// deleting node is never ready.
	g.Upsert("ns/c", nil, true, true)
	_, notReady = g.Blockers("ns/a")
	if len(notReady) != 1 || notReady[0] != "ns/c" {
		t.Fatalf("deleting c must block: notReady=%v", notReady)
	}
}

func TestMissingErrorIsDistinguishable(t *testing.T) {
	err := &MissingError{Key: "ns/a", Missing: []string{"ns/d"}}
	if err.Error() == "" {
		t.Fatal("empty error")
	}
	var me *MissingError
	if !errors.As(err, &me) {
		t.Fatal("must be detectable as *MissingError")
	}
}

func TestCycleDetectionAndErrors(t *testing.T) {
	g := NewGraph()
	// a -> b -> c -> a (cycle), plus a side leaf d -> a.
	g.Upsert("ns/a", []string{"ns/b"}, false, false)
	g.Upsert("ns/b", []string{"ns/c"}, false, false)
	g.Upsert("ns/c", []string{"ns/a"}, false, false)
	g.Upsert("ns/d", []string{"ns/a"}, false, false)

	cycle := g.Cycle("ns/a")
	if cycle == nil || cycle[0] != "ns/a" || cycle[len(cycle)-1] != "ns/a" {
		t.Fatalf("a must detect a closed loop, got %v", cycle)
	}
	// d reaches the cycle through a, so d reports a cycle too.
	if g.Cycle("ns/d") == nil {
		t.Fatal("d transitively participates in the cycle")
	}
	t.Logf("INPUT a->b->c->a, d->a | JUDGE: cycle(a)=%v cycle(d)!=nil", cycle)

	ce := &CycleError{Key: "ns/a", Cycle: cycle}
	var target *CycleError
	if !errors.As(ce, &target) {
		t.Fatal("cycle error must be detectable")
	}

	// Break edge c -> a: no cycle anywhere.
	g.Upsert("ns/c", nil, false, false)
	if g.Cycle("ns/a") != nil || g.Cycle("ns/d") != nil {
		t.Fatal("cycle must be gone after breaking the edge")
	}
}

func TestSelfCycle(t *testing.T) {
	g := NewGraph()
	g.Upsert("ns/x", []string{"ns/x"}, false, false)
	if c := g.Cycle("ns/x"); len(c) != 2 || c[0] != "ns/x" || c[1] != "ns/x" {
		t.Fatalf("self dependency must be a cycle, got %v", c)
	}
}

func TestDeletePropagatesToDependentsAndKeepsRefs(t *testing.T) {
	g := NewGraph()
	g.Upsert("ns/dep", nil, true, false)
	g.Upsert("ns/a", []string{"ns/dep"}, false, false)
	g.Upsert("ns/b", []string{"ns/dep"}, false, false)

	deps, upstreams := g.Delete("ns/dep")
	if len(deps) != 2 || deps[0] != "ns/a" || deps[1] != "ns/b" {
		t.Fatalf("dependents=%v want [a b]", deps)
	}
	// dep declared no dependencies itself.
	if len(upstreams) != 0 {
		t.Fatalf("dependencies=%v want empty", upstreams)
	}
	// a now observes a missing dependency.
	missing, _ := g.Blockers("ns/a")
	if len(missing) != 1 || missing[0] != "ns/dep" {
		t.Fatalf("missing after delete=%v", missing)
	}
	// Recreate dep: the retained reverse index still finds both dependents.
	g.Upsert("ns/dep", nil, true, false)
	if again := g.Dependents("ns/dep"); len(again) != 2 {
		t.Fatalf("dependents after recreate=%v want 2", again)
	}
	missing, _ = g.Blockers("ns/a")
	if len(missing) != 0 {
		t.Fatalf("recreated dep must resolve missing, got %v", missing)
	}
}

func TestUpsertEdgeChangeUpdatesReverseIndex(t *testing.T) {
	g := NewGraph()
	g.Upsert("ns/a", []string{"ns/x", "ns/y"}, true, false)
	if got := g.Dependents("ns/x"); len(got) != 1 || got[0] != "ns/a" {
		t.Fatalf("dependents(x)=%v", got)
	}
	// a drops x, keeps y and adds z.
	g.Upsert("ns/a", []string{"ns/y", "ns/z"}, true, false)
	if got := g.Dependents("ns/x"); len(got) != 0 {
		t.Fatalf("x must no longer be referenced, got %v", got)
	}
	if got := g.Dependents("ns/y"); len(got) != 1 {
		t.Fatalf("y still referenced, got %v", got)
	}
	if got := g.Dependents("ns/z"); len(got) != 1 {
		t.Fatalf("z newly referenced, got %v", got)
	}
}

func TestReplaceBuildsCompleteGraph(t *testing.T) {
	g := NewGraph()
	g.Upsert("stale", nil, true, false)
	g.Replace([]SnapshotEntry{
		{Key: "ns/a", Deps: []string{"ns/b"}, Ready: false},
		{Key: "ns/b", Ready: false},
	})
	if _, notReady := g.Blockers("ns/a"); len(notReady) != 1 || notReady[0] != "ns/b" {
		t.Fatal("replace snapshot must gate a")
	}
	if g.Dependents("ns/b")[0] != "ns/a" {
		t.Fatal("replace must build reverse index")
	}
	if got := g.Dependents("stale"); len(got) != 0 {
		t.Fatalf("stale node must be gone, got %v", got)
	}
}

func TestDeleteDependentCleansReverseIndex(t *testing.T) {
	g := NewGraph()
	g.Upsert("ns/dep", nil, true, false)
	g.Upsert("ns/a", []string{"ns/dep"}, false, false)
	if dependents, dependencies := g.Delete("ns/a"); len(dependents) != 0 ||
		len(dependencies) != 1 || dependencies[0] != "ns/dep" {
		t.Fatalf("deleting dependent must clear ref and report its dep; got dependents=%v dependencies=%v",
			dependents, dependencies)
	}
	if got := g.Dependents("ns/dep"); len(got) != 0 {
		t.Fatalf("deleting dependent must clear ref, got %v", got)
	}
}

func TestUpsertReturnsNewlyAddedUpstream(t *testing.T) {
	g := NewGraph()
	added := g.Upsert("ns/a", []string{"ns/x", "ns/y"}, false, false)
	if len(added) != 2 || added[0] != "ns/x" || added[1] != "ns/y" {
		t.Fatalf("added=%v want [ns/x ns/y]", added)
	}
	// Re-upserting the same edges reports nothing new.
	if added := g.Upsert("ns/a", []string{"ns/x", "ns/y"}, true, false); len(added) != 0 {
		t.Fatalf("unchanged edges must report no new upstream, got %v", added)
	}
	// Dropping x and adding z reports only z.
	added = g.Upsert("ns/a", []string{"ns/y", "ns/z"}, true, false)
	if len(added) != 1 || added[0] != "ns/z" {
		t.Fatalf("added=%v want [ns/z]", added)
	}
	// x is no longer referenced by anyone.
	if got := g.Dependents("ns/x"); len(got) != 0 {
		t.Fatalf("x should have no dependents, got %v", got)
	}
	t.Log("INPUT edge churn a:{x,y}->{y,z} | JUDGE: only z reported as new upstream")
}
