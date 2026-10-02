package runtime

import "context"

// HandlerFunc 是最常见的进程内函数形态：输入任意值，返回输出与业务错误。
type HandlerFunc func(ctx context.Context, in any) (any, error)

// FuncInstance 把一个 HandlerFunc 包装成 Instance。Close 可选。
type FuncInstance struct {
	handler HandlerFunc
	onClose func() error
}

// NewFuncInstance 创建一个绑定 handler 的实例。onClose 可为 nil。
func NewFuncInstance(handler HandlerFunc, onClose func() error) *FuncInstance {
	return &FuncInstance{handler: handler, onClose: onClose}
}

func (i *FuncInstance) Execute(ctx context.Context, in any) (any, error) {
	return i.handler(ctx, in)
}

func (i *FuncInstance) Close() error {
	if i.onClose != nil {
		return i.onClose()
	}
	return nil
}

// NewFuncFactory 根据“函数名/版本 -> handler”的注册表生成 Factory。
// 未登记的版本会返回 INSTANCE_CREATE_FAILED 错误，便于演示创建失败路径。
func NewFuncFactory(handlers map[string]map[string]HandlerFunc) Factory {
	return func(spec VersionSpec) (Instance, error) {
		if versions, ok := handlers[spec.Function]; ok {
			if h, ok2 := versions[spec.Version]; ok2 {
				return NewFuncInstance(h, nil), nil
			}
		}
		return nil, platformError(ErrInstanceCreate,
			"no handler registered for %q@%s", spec.Function, spec.Version)
	}
}
