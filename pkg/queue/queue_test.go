package queue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func drainOnce(t *testing.T, q *Queue) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	k, shutdown, err := q.Get(ctx)
	if err != nil || shutdown {
		t.Fatalf("Get: key=%q shutdown=%v err=%v", k, shutdown, err)
	}
	return k
}

func TestDedupAndMergeDuringProcessing(t *testing.T) {
	q := New(0, 0)
	for i := 0; i < 5; i++ {
		if err := q.Add("r1"); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("输入: 连续 5 次 Add(r1) => 判定: Len=%d（期望 1，重复入队不产生重复条目）", q.Len())
	if q.Len() != 1 {
		t.Fatalf("Len=%d, want 1", q.Len())
	}

	k := drainOnce(t, q)
	if k != "r1" {
		t.Fatalf("got %q", k)
	}
	// 处理期间连续变更：全部合并为一个 dirty 标记。
	for i := 0; i < 3; i++ {
		_ = q.Add("r1")
	}
	t.Logf("输入: 处理中 Add(r1) x3 => 判定: Len=%d（处理中不计入等待，仅合并）", q.Len())
	if q.Len() != 0 {
		t.Fatalf("处理中不应出现在等待列表, Len=%d", q.Len())
	}
	q.Done("r1", 0)
	if q.Len() != 1 {
		t.Fatalf("Done 后合并变更应产生恰好 1 个条目, Len=%d", q.Len())
	}
	k = drainOnce(t, q)
	q.Done(k, 0)
	t.Log("判定依据: 同键任一时刻最多存在于一处；处理中变更在 Done 时合并")
}

func TestFIFOOrderAcrossDifferentKeys(t *testing.T) {
	q := New(0, 0)
	for _, k := range []string{"a", "b", "c"} {
		_ = q.Add(k)
	}
	var got []string
	for range []int{1, 2, 3} {
		k := drainOnce(t, q)
		got = append(got, k)
		q.Done(k, 0)
	}
	t.Logf("输入: Add(a,b,c) => 判定: 取出顺序=%v（期望 [a b c]，任务按入队顺序处理）", got)
	if fmt.Sprint(got) != "[a b c]" {
		t.Fatalf("order=%v", got)
	}
}

func TestExponentialBackoffMonotonic(t *testing.T) {
	b := NewExponentialBackoff(10*time.Millisecond, 100*time.Millisecond, 2)
	var delays []time.Duration
	for i := 1; i <= 6; i++ {
		_, d := b.Fail("r")
		delays = append(delays, d)
	}
	t.Logf("输入: 同一键连续失败 6 次 base=10ms factor=2 max=100ms => 判定: 退避序列=%v", delays)
	for i := 1; i < len(delays); i++ {
		if delays[i] < delays[i-1] {
			t.Fatalf("退避必须单调不减: %v < %v", delays[i], delays[i-1])
		}
	}
	if delays[0] != 10*time.Millisecond || delays[len(delays)-1] != 100*time.Millisecond {
		t.Fatalf("首退避=%v 封顶=%v", delays[0], delays[len(delays)-1])
	}
	b.Reset("r")
	_, d := b.Fail("r")
	if d != 10*time.Millisecond {
		t.Fatalf("Reset 后应从 base 重新开始, got %v", d)
	}
	t.Log("判定依据: 每次 ×2 直到封顶；成功后 Reset 回到 base")
}

func TestBackoffRequeueAndPromotion(t *testing.T) {
	q := New(0, 0)
	_ = q.Add("slow")
	k := drainOnce(t, q)
	q.Done(k, 30*time.Millisecond) // 失败，30ms 后重试
	t.Logf("输入: Done(slow, 30ms) => 判定: 立即 Len=%d（延迟中），等待到期", q.Len())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, shutdown, err := q.Get(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("退避未到期不应取出, shutdown=%v err=%v", shutdown, err)
	}
	time.Sleep(35 * time.Millisecond)
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	k2, _, err := q.Get(ctx2)
	if err != nil || k2 != "slow" {
		t.Fatalf("到期后应取出 slow, got %q err=%v", k2, err)
	}
	q.Done(k2, 0)
	t.Log("判定依据: 延迟条目到期后才提升到就绪，退避期间 Get 阻塞")
}

func TestRateLimit(t *testing.T) {
	q := New(50, 0) // 每秒 50 个 => 间隔 20ms
	_ = q.Add("x")
	_ = q.Add("y")
	start := time.Now()
	k := drainOnce(t, q)
	q.Done(k, 0)
	k = drainOnce(t, q)
	q.Done(k, 0)
	elapsed := time.Since(start)
	t.Logf("输入: rate=50/s 连续取 2 个 => 判定: 耗时=%v（应 >=20ms，限流生效）", elapsed)
	if elapsed < 18*time.Millisecond {
		t.Fatalf("限流未生效: %v", elapsed)
	}
}

func TestBoundedCapacityBackpressure(t *testing.T) {
	q := New(0, 2)
	if err := q.Add("a"); err != nil {
		t.Fatal(err)
	}
	if err := q.Add("b"); err != nil {
		t.Fatal(err)
	}
	err := q.Add("c")
	t.Logf("输入: 容量 2 的队列 Add(a,b,c) => 判定: 第三次 err=%v（期望 ErrQueueFull，背压不丢已有任务）", err)
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("got %v, want ErrQueueFull", err)
	}
	k := drainOnce(t, q)
	q.Done(k, 0)
	if err := q.Add("c"); err != nil {
		t.Fatalf("腾出容量后应可入队: %v", err)
	}
	t.Log("判定依据: 容量满 => ErrQueueFull 显式背压，队列不会无限增长")
}

func TestShutdownDrainsWithoutLosingTasks(t *testing.T) {
	q := New(0, 0)
	_ = q.Add("a")
	_ = q.Add("b")
	// 一个正在处理中的键，带退避延迟；关闭时应被保留并尽快排空。
	k := drainOnce(t, q)
	q.Done(k, 5*time.Minute)

	q.Shutdown()
	t.Log("输入: 队列含 a,b 与 5 分钟退避的 a => Shutdown() => 判定: 延迟立即提升，全部排空后 shutdown")

	err := q.Add("new")
	if !errors.Is(err, ErrQueueStopped) {
		t.Fatalf("关闭后 Add 应 ErrQueueStopped, got %v", err)
	}

	seen := map[string]int{}
	for i := 0; i < 2; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		key, shutdown, gerr := q.Get(ctx)
		cancel()
		if gerr != nil {
			t.Fatalf("Get %d: %v", i, gerr)
		}
		if shutdown {
			t.Logf("第 %d 次 Get: shutdown（已排空）", i+1)
			if i != 2 {
				t.Fatalf("排空前不应 shutdown, i=%d seen=%v", i, seen)
			}
			break
		}
		seen[key]++
		q.Done(key, 0)
		t.Logf("排空取出: %q", key)
	}
	// a 已在首次 Get 时被处理一次；关闭只应保留其退避重入副本 + b，各 1 次。
	if seen["a"] != 1 || seen["b"] != 1 {
		t.Fatalf("任务丢失或重复: seen=%v", seen)
	}

	// 排空后持续 Get 立即返回 shutdown。
	_, shutdown, _ := q.Get(context.Background())
	if !shutdown {
		t.Fatal("排空后应持续返回 shutdown")
	}
	t.Log("判定依据: 关闭不丢任务（退避中的 a 与等待中的 b 各排空 1 次），排空后 Get 返回 shutdown")
}

func TestConcurrentAddAndDrain(t *testing.T) {
	q := New(100000, 0)
	const n = 200
	var wg sync.WaitGroup
	// 每个键被 3 个 goroutine 同时 Add，去重后每个键最多处理一次/轮。
	wg.Add(n * 3)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("k-%d", i)
		for j := 0; j < 3; j++ {
			go func() {
				defer wg.Done()
				_ = q.Add(key)
			}()
		}
	}
	wg.Wait()
	t.Logf("输入: %d 个键各被并发 Add 3 次 => 判定: Len=%d（期望 %d）", n, q.Len(), n)
	if q.Len() != n {
		t.Fatalf("Len=%d want %d", q.Len(), n)
	}
}
