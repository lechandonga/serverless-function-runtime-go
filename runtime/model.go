package runtime

import (
	"time"
)

// 缺省配置：注册时零值字段会被填充为这些默认值。
const (
	defaultMaxConcurrency = 8
	defaultMaxQueue       = 16
	defaultInvokeTimeout  = 30 * time.Second
	defaultQueueTimeout   = 10 * time.Second
	defaultInstanceIdle   = 5 * time.Minute
	defaultReapInterval   = time.Second
	defaultDedupTTL       = 10 * time.Minute
)

// FunctionConfig 是单个函数的并发与生命周期配置。
type FunctionConfig struct {
	// MaxConcurrency 函数级并发上限，所有版本共享。
	MaxConcurrency int
	// MaxQueue 满负荷时允许排队的调用数，超出立即 OVERLOADED。
	MaxQueue int
	// MaxInstancesPerVersion 每个版本最多热实例数（含忙碌与空闲）。
	MaxInstancesPerVersion int
	// InvokeTimeout 单次执行进入执行器后的超时时间。
	InvokeTimeout time.Duration
	// QueueTimeout 等待并发名额/实例的最长排队时间。
	QueueTimeout time.Duration
	// InstanceIdleTimeout 实例空闲多久后被回收。
	InstanceIdleTimeout time.Duration
}

func (c FunctionConfig) withDefaults() FunctionConfig {
	if c.MaxConcurrency == 0 {
		c.MaxConcurrency = defaultMaxConcurrency
	}
	if c.MaxQueue == 0 {
		c.MaxQueue = defaultMaxQueue
	}
	if c.MaxInstancesPerVersion == 0 {
		c.MaxInstancesPerVersion = c.MaxConcurrency
	}
	if c.InvokeTimeout == 0 {
		c.InvokeTimeout = defaultInvokeTimeout
	}
	if c.QueueTimeout == 0 {
		c.QueueTimeout = defaultQueueTimeout
	}
	if c.InstanceIdleTimeout == 0 {
		c.InstanceIdleTimeout = defaultInstanceIdle
	}
	return c
}

func (c FunctionConfig) validate() error {
	switch {
	case c.MaxConcurrency < 1:
		return platformError(ErrInvalidConfig, "MaxConcurrency must be >= 1, got %d", c.MaxConcurrency)
	case c.MaxQueue < 0:
		return platformError(ErrInvalidConfig, "MaxQueue must be >= 0, got %d", c.MaxQueue)
	case c.MaxInstancesPerVersion < 1:
		return platformError(ErrInvalidConfig, "MaxInstancesPerVersion must be >= 1, got %d", c.MaxInstancesPerVersion)
	case c.InvokeTimeout <= 0:
		return platformError(ErrInvalidConfig, "InvokeTimeout must be > 0, got %s", c.InvokeTimeout)
	case c.QueueTimeout <= 0:
		return platformError(ErrInvalidConfig, "QueueTimeout must be > 0, got %s", c.QueueTimeout)
	case c.InstanceIdleTimeout <= 0:
		return platformError(ErrInvalidConfig, "InstanceIdleTimeout must be > 0, got %s", c.InstanceIdleTimeout)
	}
	return nil
}

// VersionSpec 是创建实例时交给工厂的不可变版本快照。
// 已经开始的调用始终引用它开始时拿到的同一个 spec/pool，版本更新不影响在途调用。
type VersionSpec struct {
	Function string
	Version  string
}

// Status 是调用归一化后的结论状态。
type Status string

const (
	// StatusOK 执行器正常返回。
	StatusOK Status = "OK"
	// StatusFailed 执行器以业务错误返回（非平台问题）。
	StatusFailed Status = "FAILED"
	// StatusPlatformError 平台错误：过载、超时、实例创建失败、panic 等。
	StatusPlatformError Status = "PLATFORM_ERROR"
)

// Result 是一次调用的稳定结论。幂等重放返回的 Result 与首次完全一致。
type Result struct {
	Status Status
	Output any
	// Err 解释失败原因：StatusFailed 时是执行器返回的业务错误；
	// StatusPlatformError 时一定是 *PlatformError。
	Err error
	// Version 是本次调用实际执行的版本（按调用开始时的路由快照解析）。
	Version string
}

// InvokeOptions 控制单次调用的路由与超时；零值字段取函数配置默认值。
type InvokeOptions struct {
	// Version 指定要调用的版本；为空时路由到当前版本。
	Version string
	// CallID 非空时启用幂等：相同 CallID 全局只真正执行一次。
	CallID string
	// InvokeTimeout / QueueTimeout 为 0 时使用函数配置。
	InvokeTimeout time.Duration
	QueueTimeout  time.Duration
}

// Options 是运行时级别的选项。
type Options struct {
	// Clock 默认使用系统时钟；测试可注入 FakeClock 做确定性推进。
	Clock Clock
	// ReapInterval 空闲回收扫描周期，默认 1s。
	ReapInterval time.Duration
	// DedupTTL 幂等记录保留时长，默认 10m。
	DedupTTL time.Duration
}
