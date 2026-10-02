package runtime

import "fmt"

// ErrorCode 是平台归一化后的错误码。执行器自身的业务错误不会被包装成
// 平台错误（调用结果状态为 StatusFailed），只有平台侧问题才使用这些错误码。
type ErrorCode string

const (
	// ErrInvalidConfig 注册/更新时定义或配置不合法，整体拒绝、状态不变。
	ErrInvalidConfig ErrorCode = "INVALID_CONFIG"
	// ErrFunctionNotFound 函数名不存在。
	ErrFunctionNotFound ErrorCode = "FUNCTION_NOT_FOUND"
	// ErrVersionUnknown 版本号从未发布过（与“存在但已停用”区分）。
	ErrVersionUnknown ErrorCode = "VERSION_UNKNOWN"
	// ErrVersionDeactivated 版本存在但已被停用。
	ErrVersionDeactivated ErrorCode = "VERSION_DEACTIVATED"
	// ErrOverloaded 函数并发已达上限且排队名额也已满（快速失败）。
	ErrOverloaded ErrorCode = "OVERLOADED"
	// ErrQueuedTimeout 在队列中等待并发名额超时，调用从未进入执行器。
	ErrQueuedTimeout ErrorCode = "QUEUED_TIMEOUT"
	// ErrInvocationTimeout 执行器在超时时间内没有返回；结果已定，迟到结果被丢弃。
	ErrInvocationTimeout ErrorCode = "INVOCATION_TIMEOUT"
	// ErrInstanceCreate 创建执行实例失败。
	ErrInstanceCreate ErrorCode = "INSTANCE_CREATE_FAILED"
)

// PlatformError 是运行时对外暴露的、可按 Code 区分处理的平台错误。
type PlatformError struct {
	Code    ErrorCode
	Message string
	// Cause 保存底层错误（例如执行器工厂返回的错误），可能为 nil。
	Cause error
}

func (e *PlatformError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s: %s: %v", e.Code, e.Message, e.Cause)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func (e *PlatformError) Unwrap() error { return e.Cause }

func platformError(code ErrorCode, format string, args ...any) *PlatformError {
	return &PlatformError{Code: code, Message: fmt.Sprintf(format, args...)}
}

// normalizePanic 把执行器 panic 抛出的任意值归一化成 error。
func normalizePanic(r any) error {
	if r == nil {
		return nil
	}
	if err, ok := r.(error); ok {
		return err
	}
	return fmt.Errorf("executor panic: %v", r)
}
