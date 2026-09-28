package reconcile

import (
	"context"
	"errors"
	"testing"

	"github.com/xingjianluo/reconcile-engine-go/internal/object"
	"github.com/xingjianluo/reconcile-engine-go/internal/store"
)

func putObject(t *testing.T, ctx context.Context, s store.Store, ns, name string, deps ...string) *object.Object {
	t.Helper()
	o, err := s.Create(ctx, &object.Object{
		Namespace: ns, Name: name, DependsOn: deps,
		Spec: map[string]string{"k": "v"},
	})
	if err != nil {
		t.Fatal(err)
	}
	return o
}

func TestDefaultReady(t *testing.T) {
	cases := []struct {
		name string
		obj  *object.Object
		want bool
	}{
		{"both empty", &object.Object{}, true},
		{"status mirrors spec", &object.Object{
			Spec: map[string]string{"a": "1"}, Status: map[string]string{"a": "1"},
		}, true},
		{"status lagging", &object.Object{
			Spec: map[string]string{"a": "1"}, Status: map[string]string{},
		}, false},
		{"status extra key", &object.Object{
			Spec: map[string]string{"a": "1"}, Status: map[string]string{"a": "1", "b": "2"},
		}, false},
		{"value differs", &object.Object{
			Spec: map[string]string{"a": "1"}, Status: map[string]string{"a": "2"},
		}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := DefaultReady(c.obj); got != c.want {
				t.Fatalf("DefaultReady = %v, want %v", got, c.want)
			}
		})
	}
}

func TestFindCycle(t *testing.T) {
	ctx := context.Background()

	t.Run("acyclic chain", func(t *testing.T) {
		s := store.NewMemoryStore()
		putObject(t, ctx, s, "ns", "a", "ns/b")
		putObject(t, ctx, s, "ns", "b", "ns/c")
		putObject(t, ctx, s, "ns", "c")
		if p := findCycle(ctx, s, "ns/a"); p != nil {
			t.Fatalf("unexpected cycle: %v", p)
		}
	})
	t.Run("self loop", func(t *testing.T) {
		s := store.NewMemoryStore()
		putObject(t, ctx, s, "ns", "a", "ns/a")
		p := findCycle(ctx, s, "ns/a")
		if len(p) < 2 || p[0] != "ns/a" || p[len(p)-1] != "ns/a" {
			t.Fatalf("malformed self-loop path: %v", p)
		}
	})
	t.Run("two node cycle", func(t *testing.T) {
		s := store.NewMemoryStore()
		putObject(t, ctx, s, "ns", "a", "ns/b")
		putObject(t, ctx, s, "ns", "b", "ns/a")
		p := findCycle(ctx, s, "ns/a")
		if p == nil || len(p) != 3 {
			t.Fatalf("want path of 3 keys, got %v", p)
		}
	})
	t.Run("cycle downstream reachable", func(t *testing.T) {
		s := store.NewMemoryStore()
		putObject(t, ctx, s, "ns", "a", "ns/b")
		putObject(t, ctx, s, "ns", "b", "ns/c")
		putObject(t, ctx, s, "ns", "c", "ns/b")
		if p := findCycle(ctx, s, "ns/a"); p == nil {
			t.Fatal("downstream cycle must be visible from upstream node")
		}
	})
	t.Run("missing node ends branch not cycle", func(t *testing.T) {
		s := store.NewMemoryStore()
		putObject(t, ctx, s, "ns", "a", "ns/ghost")
		if p := findCycle(ctx, s, "ns/a"); p != nil {
			t.Fatalf("missing dependency is not a cycle, got %v", p)
		}
	})
}

func TestEvaluateDeps(t *testing.T) {
	ctx := context.Background()

	t.Run("all ready", func(t *testing.T) {
		s := store.NewMemoryStore()
		a := putObject(t, ctx, s, "ns", "a", "ns/b")
		b := putObject(t, ctx, s, "ns", "b")
		b, _ = s.Update(ctx, b.Key(), b.ResourceVersion, func(o *object.Object) {
			o.Status = map[string]string{"k": "v"}
		})
		got, err := evaluateDeps(ctx, s, a, DefaultReady)
		if got != depOK || err != nil {
			t.Fatalf("want depOK nil, got %v %v", got, err)
		}
	})
	t.Run("not ready waits", func(t *testing.T) {
		s := store.NewMemoryStore()
		a := putObject(t, ctx, s, "ns", "a", "ns/b")
		putObject(t, ctx, s, "ns", "b") // status empty, spec k=v -> not ready
		got, err := evaluateDeps(ctx, s, a, DefaultReady)
		if got != depWait || err != nil {
			t.Fatalf("want depWait nil, got %v %v", got, err)
		}
	})
	t.Run("dependency being deleted waits", func(t *testing.T) {
		s := store.NewMemoryStore()
		a := putObject(t, ctx, s, "ns", "a", "ns/b")
		b := putObject(t, ctx, s, "ns", "b")
		_, _ = s.Update(ctx, b.Key(), b.ResourceVersion, func(o *object.Object) {
			o.AddFinalizer("f")
			o.Status = map[string]string{"k": "v"}
		})
		b, _ = s.Get(ctx, b.Key())
		b, _ = s.Delete(ctx, b.Key(), b.ResourceVersion)
		got, err := evaluateDeps(ctx, s, a, DefaultReady)
		if got != depWait || err != nil {
			t.Fatalf("want depWait nil for deleting dependency, got %v %v", got, err)
		}
	})
	t.Run("missing dependency terminal", func(t *testing.T) {
		s := store.NewMemoryStore()
		a := putObject(t, ctx, s, "ns", "a", "ns/ghost")
		got, err := evaluateDeps(ctx, s, a, DefaultReady)
		if got != depMissing {
			t.Fatalf("want depMissing, got %v", got)
		}
		var dm *DependencyMissingError
		if !errors.As(err, &dm) || dm.Dependency != "ns/ghost" {
			t.Fatalf("want *DependencyMissingError, got %v", err)
		}
	})
	t.Run("cycle takes precedence over readiness", func(t *testing.T) {
		s := store.NewMemoryStore()
		a := putObject(t, ctx, s, "ns", "a", "ns/a") // self loop, not ready
		got, err := evaluateDeps(ctx, s, a, DefaultReady)
		if got != depCycle {
			t.Fatalf("want depCycle before depWait, got %v", got)
		}
		var dc *DependencyCycleError
		if !errors.As(err, &dc) {
			t.Fatalf("want *DependencyCycleError, got %v", err)
		}
	})
	t.Run("no dependencies", func(t *testing.T) {
		s := store.NewMemoryStore()
		a := putObject(t, ctx, s, "ns", "a")
		if got, err := evaluateDeps(ctx, s, a, DefaultReady); got != depOK || err != nil {
			t.Fatalf("want depOK, got %v %v", got, err)
		}
	})
}

func TestLiveDependents(t *testing.T) {
	ctx := context.Background()
	s := store.NewMemoryStore()
	putObject(t, ctx, s, "ns", "base")
	putObject(t, ctx, s, "ns", "d1", "ns/base")
	putObject(t, ctx, s, "ns", "d2", "ns/base", "ns/other")
	putObject(t, ctx, s, "ns", "unrelated")

	got, err := liveDependents(ctx, s, "ns/base")
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, k := range got {
		seen[k] = true
	}
	if !seen["ns/d1"] || !seen["ns/d2"] || seen["ns/base"] || seen["ns/unrelated"] {
		t.Fatalf("unexpected dependent set: %v", got)
	}
}
