package runtime

import "fmt"

// ErrorKind 是平台归一化后的错误类别。
// 所有调用失败都可以通过 ErrorKind 稳定、可区分地判定原因，
// 与具体执行器实现、错误文本无关，便于复现与断言。
type ErrorKind string

const (
	// KindValidation 注册/更新的函数定义或配置非法，整体被拒绝。
	KindValidation ErrorKind = "ValidationRejected"
	// KindFunctionNotFound 函数名未注册。
	KindFunctionNotFound ErrorKind = "FunctionNotFound"
	// KindVersionUnknown 指定的版本号从未发布过（与“已停用”区分）。
	KindVersionUnknown ErrorKind = "VersionUnknown"
	// KindVersionDisabled 版本曾经存在，但已被停用，不再接受新调用。
	KindVersionDisabled ErrorKind = "VersionDisabled"
	// KindOverload 并发槽与排队名额都已占满，调用被过载拒绝（快速失败）。
	KindOverload ErrorKind = "Overloaded"
	// KindQueueTimeout 调用在队列中等待实例，但未在截止时间前获得派发。
	KindQueueTimeout ErrorKind = "QueueTimeout"
	// KindInvokeTimeout 调用已派发执行，但执行器未在超时时间内返回。
	KindInvokeTimeout ErrorKind = "InvokeTimeout"
	// KindCanceled 调用方在得到结论前主动取消了调用。
	KindCanceled ErrorKind = "Canceled"
	// KindExecutorFailed 执行器正常返回了一个业务错误。
	KindExecutorFailed ErrorKind = "ExecutorFailed"
	// KindExecutorCrashed 执行器发生 panic / 崩溃，被平台兜底恢复。
	KindExecutorCrashed ErrorKind = "ExecutorCrashed"
)

// PlatformError 是运行时对外返回的统一错误类型。
type PlatformError struct {
	Kind     ErrorKind
	Function string
	Version  Version
	Message  string
	Cause    error
}

func (e *PlatformError) Error() string {
	base := string(e.Kind)
	if e.Function != "" {
		base += " function=" + e.Function
	}
	if e.Version != "" {
		base += " version=" + string(e.Version)
	}
	if e.Message != "" {
		base += ": " + e.Message
	}
	if e.Cause != nil {
		base += ": " + e.Cause.Error()
	}
	return base
}

func (e *PlatformError) Unwrap() error { return e.Cause }

func platformError(kind ErrorKind, fn string, v Version, format string, args ...any) *PlatformError {
	return &PlatformError{
		Kind:     kind,
		Function: fn,
		Version:  v,
		Message:  fmt.Sprintf(format, args...),
	}
}
