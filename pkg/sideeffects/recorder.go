package sideeffects

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
)

// Recorder 以「资源名 + 资源版本」为幂等键记录已发生的副作用，
// 保证同一版本被重复调谐（重复入队、重试、多 worker）时不会重复执行。
//
// 资源版本变化（ResourceVersion 变更，包括删除标记）后允许再次执行，
// 这与「过期版本的操作必须被识别」配合：调谐方先读到最新版本，
// 再以该版本登记副作用，冲突重试自然只在新版本上生效一次。
// inflight 标记「已预占、fn 尚在执行」的键，避免重叠调用重复执行。
type Recorder struct {
	mu       sync.Mutex
	done     map[string]struct{}
	inflight map[string]struct{}
}

// New 创建副作用记录器。
func New() *Recorder {
	return &Recorder{done: map[string]struct{}{}, inflight: map[string]struct{}{}}
}

// key 拼装幂等键：长度前缀避免名称中可能出现的分隔符歧义。
func idKey(name string, rv uint64) string {
	return fmt.Sprintf("%d/%s#%d", len(name), name, rv)
}

// OnceExecute 仅当 (name, rv) 首次出现时执行 fn；
// executed=true 表示本次真正执行了副作用，false 表示被幂等去重。
//
// 语义保证：
//   - 键位在锁内原子「预占」，fn 在锁外执行——即使两个 goroutine 以相同
//     (name, rv) 重叠进入，也只有一个会执行 fn，另一个立刻得到 executed=false；
//   - fn 返回错误时释放预占且不登记，下次同版本仍可重试（副作用未确认完成）；
//   - fn 执行期间的重叠调用不会排队等待，而是直接视为「已有一次在执行」而跳过。
func (r *Recorder) OnceExecute(name string, rv uint64, fn func() error) (executed bool, err error) {
	k := idKey(name, rv)
	r.mu.Lock()
	if _, ok := r.done[k]; ok {
		r.mu.Unlock()
		return false, nil
	}
	if _, ok := r.inflight[k]; ok {
		r.mu.Unlock()
		return false, nil
	}
	r.inflight[k] = struct{}{}
	r.mu.Unlock()

	if runErr := fn(); runErr != nil {
		r.mu.Lock()
		delete(r.inflight, k) // 失败释放预占，允许后续重试
		r.mu.Unlock()
		return false, runErr
	}

	r.mu.Lock()
	delete(r.inflight, k)
	r.done[k] = struct{}{}
	r.mu.Unlock()
	return true, nil
}

// ExecutedVersions 返回某资源已记录副作用的资源版本（测试/可观察性用）。
func (r *Recorder) ExecutedVersions(name string) []uint64 {
	prefix := fmt.Sprintf("%d/%s#", len(name), name)
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []uint64
	for k := range r.done {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		rv, err := strconv.ParseUint(k[len(prefix):], 10, 64)
		if err != nil {
			continue
		}
		out = append(out, rv)
	}
	return out
}

// ExecutedCount 返回某资源已记录副作用的版本数量。
func (r *Recorder) ExecutedCount(name string) int {
	return len(r.ExecutedVersions(name))
}
