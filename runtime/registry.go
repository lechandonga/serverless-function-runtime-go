package runtime

import (
	"sync"
	"sync/atomic"
	"time"
)

// functionEntry 保存单个函数的全部不可变版本、路由指针和每版本实例池。
// 所有变更都在 mu 写锁内“先构建、后提交”，任何非法更新在持锁阶段被拒绝，
// 不会留下改了一半的中间状态。
type functionEntry struct {
	mu        sync.RWMutex
	name      string
	revisions map[Version]*Revision
	current   Version
	pools     map[Version]*instancePool
}

// NewRuntime 创建一个纯本地、进程内的函数运行时。
func NewRuntime() *Runtime {
	return &Runtime{functions: map[string]*functionEntry{}, calls: map[string]*idempotentEntry{}}
}

// Runtime 是本地 Serverless 函数运行时。
// 不依赖容器、外部服务或远程资源：注册、路由、实例池、并发控制、
// 幂等去重全部发生在当前进程内。
type Runtime struct {
	mu        sync.RWMutex
	functions map[string]*functionEntry

	callsMu sync.Mutex
	calls   map[string]*idempotentEntry

	drainClaim sync.Mutex
	drain      atomic.Pointer[drainState] // 非 nil 表示正在排空或已完成排空（此后永久拒绝新调用）

	closeOnce sync.Once
	closed    bool
}

// Register 注册一个新函数并发布首个版本，同时把它设为当前版本。
func (rt *Runtime) Register(name string, spec VersionSpec) error {
	if err := validateName(name); err != nil {
		return err
	}
	if err := validateSpec(spec); err != nil {
		return err
	}

	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.closed {
		return platformError(KindValidation, name, spec.Version, "runtime is closed")
	}
	if _, ok := rt.functions[name]; ok {
		return platformError(KindValidation, name, spec.Version, "function already registered")
	}
	rev := &Revision{spec: spec, publishedAt: time.Now()}
	fn := &functionEntry{
		name:      name,
		revisions: map[Version]*Revision{spec.Version: rev},
		current:   spec.Version,
		pools:     map[Version]*instancePool{},
	}
	rt.functions[name] = fn
	return nil
}

// Publish 为已注册函数发布一个新版本，并原子地把当前版本指向它。
// 新版本非法（配置不合法、版本号已存在等）时整体拒绝，
// 旧版本与旧路由完全不受影响。
func (rt *Runtime) Publish(name string, spec VersionSpec) error {
	if err := validateName(name); err != nil {
		return err
	}
	if err := validateSpec(spec); err != nil {
		return err
	}
	fn, err := rt.lookup(name, "")
	if err != nil {
		return err
	}
	fn.mu.Lock()
	defer fn.mu.Unlock()
	if _, ok := fn.revisions[spec.Version]; ok {
		return platformError(KindValidation, name, spec.Version, "version already exists")
	}
	// 先构建完整新状态，再一次性提交。
	newRev := &Revision{spec: spec, publishedAt: time.Now()}
	newPools := make(map[Version]*instancePool, len(fn.pools)+1)
	for k, v := range fn.pools {
		newPools[k] = v
	}
	newPools[spec.Version] = newInstancePool(newRev, &rt.drain)
	fn.revisions[spec.Version] = newRev
	fn.pools = newPools
	// 旧当前版本不再是路由版本：主动回收其空闲实例，在途调用照旧跑完、
	// 跑完不再保留。新版本非法时整体拒绝，因此这里的提交不会发生在失败路径。
	if old := fn.pools[fn.current]; old != nil && fn.current != spec.Version {
		old.retire()
	}
	fn.current = spec.Version
	return nil
}

// DisableVersion 停用一个版本：停用后新调用被拒绝（KindVersionDisabled），
// 已经在途的调用仍由旧实例池服务到结束。当前版本不可停用，
// 需要先发布/切换到其他版本，避免路由指向一个不可用版本。
func (rt *Runtime) DisableVersion(name string, v Version) error {
	if v == "" || v == CurrentVersion {
		return platformError(KindValidation, name, v, "an explicit version is required")
	}
	fn, err := rt.lookup(name, v)
	if err != nil {
		return err
	}
	fn.mu.Lock()
	defer fn.mu.Unlock()
	if fn.current == v {
		return platformError(KindValidation, name, v, "cannot disable the current version; publish another version first")
	}
	rev := fn.revisions[v]
	if rev.disabled {
		return platformError(KindValidation, name, v, "version already disabled")
	}
	rev.disabled = true
	if pool := fn.pools[v]; pool != nil {
		pool.retire()
	}
	return nil
}

// CurrentVersion 返回函数当前路由指向的版本。
func (rt *Runtime) CurrentVersion(name string) (Version, error) {
	fn, err := rt.lookup(name, "")
	if err != nil {
		return "", err
	}
	fn.mu.RLock()
	defer fn.mu.RUnlock()
	return fn.current, nil
}

// resolve 返回调用应使用的不可变 revision 与实例池。
// 解析成功后，调用期间持有的是指针快照：随后注册表里发生任何
// 发布、停用、回收，都不影响这次调用按开始时的版本跑完。
func (rt *Runtime) resolve(name string, v Version) (*Revision, *instancePool, error) {
	fn, err := rt.lookup(name, v)
	if err != nil {
		return nil, nil, err
	}
	fn.mu.Lock()
	defer fn.mu.Unlock()

	if v == "" || v == CurrentVersion {
		v = fn.current
	}
	rev, ok := fn.revisions[v]
	if !ok {
		return nil, nil, platformError(KindVersionUnknown, name, v, "version was never published")
	}
	if rev.disabled {
		return nil, nil, platformError(KindVersionDisabled, name, v, "version is disabled and accepts no new invocations")
	}
	pool, ok := fn.pools[v]
	if !ok {
		pool = newInstancePool(rev, &rt.drain)
		// 若排空已抢先发布，新池从诞生起就是排空态：某个版本的第一批
		// 调用与排空撞在一起时，也不可能穿过 resolve 获得派发。
		if d := rt.currentDrain(); d != nil {
			pool.adoptDrainIfNeeded(d)
		}
		fn.pools[v] = pool
	}
	return rev, pool, nil
}

func (rt *Runtime) lookup(name string, v Version) (*functionEntry, error) {
	if err := validateName(name); err != nil {
		return nil, err
	}
	rt.mu.RLock()
	fn, ok := rt.functions[name]
	rt.mu.RUnlock()
	if !ok {
		if rt.closed {
			return nil, platformError(KindValidation, name, v, "runtime is closed")
		}
		return nil, platformError(KindFunctionNotFound, name, v, "function is not registered")
	}
	return fn, nil
}

func validateName(name string) error {
	if name == "" {
		return platformError(KindValidation, "", "", "function name must not be empty")
	}
	return nil
}

func validateSpec(spec VersionSpec) error {
	if spec.Version == "" || spec.Version == CurrentVersion {
		return platformError(KindValidation, "", spec.Version, "version must be a non-empty identifier other than %q", CurrentVersion)
	}
	if spec.Handler == nil && spec.NewInstance == nil {
		return platformError(KindValidation, "", spec.Version, "handler or new-instance factory is required")
	}
	if spec.Concurrency < 1 {
		return platformError(KindValidation, "", spec.Version, "concurrency must be >= 1")
	}
	if spec.MaxQueueDepth < 0 {
		return platformError(KindValidation, "", spec.Version, "max queue depth must be >= 0")
	}
	if spec.IdleTimeout < 0 {
		return platformError(KindValidation, "", spec.Version, "idle timeout must be >= 0")
	}
	if spec.MaxIdleInstances != nil && *spec.MaxIdleInstances < 0 {
		return platformError(KindValidation, "", spec.Version, "max idle instances must be >= 0")
	}
	return nil
}
