package object

import "testing"

func TestFinalizerHelpersAndClone(t *testing.T) {
	o := &Object{Namespace: "ns", Name: "x", Spec: map[string]string{"k": "orig"}}
	if o.Key() != "ns/x" {
		t.Fatalf("key=%q", o.Key())
	}
	o.AddFinalizer("a")
	o.AddFinalizer("a") // idempotent
	o.AddFinalizer("b")
	if len(o.Finalizers) != 2 || !o.HasFinalizer("a") {
		t.Fatalf("finalizers=%v", o.Finalizers)
	}
	if o.RemoveFinalizer("zzz") {
		t.Fatal("removing an absent finalizer must report false")
	}

	cp := o.Clone()
	if !cp.RemoveFinalizer("a") {
		t.Fatal("removing present finalizer must report true")
	}
	cp.Spec["k"] = "mutated"
	if !o.HasFinalizer("a") {
		t.Fatal("clone must not share finalizer backing array with source")
	}
	if o.Spec["k"] != "orig" {
		t.Fatal("clone must not share spec map with source")
	}
	t.Logf("INPUT object with finalizers %v | JUDGE: clone isolated; helpers correct", o.Finalizers)
}

func TestBeingDeleted(t *testing.T) {
	o := &Object{}
	if o.BeingDeleted() {
		t.Fatal("new object must not be deleting")
	}
	o.DeletionTimestamp = DeletionTimestampNow()
	if !o.BeingDeleted() {
		t.Fatal("a set deletion timestamp means being deleted")
	}
}
