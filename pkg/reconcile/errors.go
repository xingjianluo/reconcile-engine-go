package reconcile

import "fmt"

// FailureKind 对调谐失败做可区分的分类。
type FailureKind string

const (
	// KindTransient 瞬时错误（如下游暂时不可用）：按退避重试。
	KindTransient FailureKind = "Transient"
	// KindConflict 版本冲突：本地视图过期，立即重读后重试。
	KindConflict FailureKind = "Conflict"
	// KindTerminal 不可恢复错误：重试无意义，记入失败结论。
	KindTerminal FailureKind = "Terminal"
	// KindExhausted 退避重试次数耗尽：给出明确失败结论。
	KindExhausted FailureKind = "Exhausted"
)

// Error 携带失败分类与原因，供控制器决定重试/终止。
type Error struct {
	Kind  FailureKind
	Cause error
	Msg   string
}

func (e *Error) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s: %s: %v", e.Kind, e.Msg, e.Cause)
	}
	return fmt.Sprintf("%s: %s", e.Kind, e.Msg)
}

func (e *Error) Unwrap() error { return e.Cause }

// NewError 构造分类错误。
func NewError(kind FailureKind, msg string, cause error) *Error {
	return &Error{Kind: kind, Msg: msg, Cause: cause}
}
