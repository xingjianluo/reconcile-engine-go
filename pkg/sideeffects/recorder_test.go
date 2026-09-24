package sideeffects

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

func TestOnceExecuteIdempotentPerVersion(t *testing.T) {
	r := New()
	var calls int32
	fn := func() error { atomic.AddInt32(&calls, 1); return nil }

	for i := 0; i < 5; i++ {
		exec, err := r.OnceExecute("svc", 7, fn)
		if err != nil || (i == 0 && !exec) || (i > 0 && exec) {
			t.Fatalf("第 %d 次: executed=%v err=%v", i+1, exec, err)
		}
	}
	t.Logf("输入: (svc,RV=7) 执行 5 次 => 判定: 真实副作用次数=%d（期望 1）", calls)
	if calls != 1 {
		t.Fatalf("calls=%d, want 1", calls)
	}

	// 新版本允许再执行一次。
	exec, _ := r.OnceExecute("svc", 8, fn)
	if !exec || calls != 2 {
		t.Fatalf("新版本应再执行: executed=%v calls=%d", exec, calls)
	}
	t.Logf("输入: (svc,RV=8) => 判定: 真实副作用次数=%d（版本变化允许新副作用），已记录版本=%v",
		calls, r.ExecutedVersions("svc"))
}

func TestOnceExecuteFailedNotRecorded(t *testing.T) {
	r := New()
	boom := errors.New("boom")
	var calls int
	exec, err := r.OnceExecute("x", 1, func() error { calls++; return boom })
	if exec || !errors.Is(err, boom) {
		t.Fatalf("失败时 executed=%v err=%v", exec, err)
	}
	exec, err = r.OnceExecute("x", 1, func() error { calls++; return nil })
	t.Logf("输入: 同版本先失败后成功 => 判定: 调用=%d 最终 executed=%v err=%v（失败不登记，可重试）",
		calls, exec, err)
	if !exec || err != nil || calls != 2 || r.ExecutedCount("x") != 1 {
		t.Fatalf("calls=%d exec=%v err=%v count=%d", calls, exec, err, r.ExecutedCount("x"))
	}
}

func TestRecorderDifferentNamesIsolated(t *testing.T) {
	r := New()
	var a, b int64
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, _ = r.OnceExecute("a", 1, func() error { atomic.AddInt64(&a, 1); return nil })
		}()
		go func() {
			defer wg.Done()
			_, _ = r.OnceExecute("b", 1, func() error { atomic.AddInt64(&b, 1); return nil })
		}()
	}
	wg.Wait()
	t.Logf("输入: a/b 各 10 次并发登记 => 判定: a 副作用=%d b 副作用=%d", a, b)
	if a == 0 || b == 0 {
		t.Fatal("不同资源的副作用应相互独立")
	}
}
