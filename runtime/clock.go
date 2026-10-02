package runtime

import (
	"sync"
	"time"
)

// Clock 抽象时间，方便在测试中确定性地推进空闲回收与超时判定。
type Clock interface {
	Now() time.Time
	// After 返回一个在 d 之后触发的通道。
	After(d time.Duration) <-chan time.Time
}

// realClock 直接使用系统时钟与 time.After。
type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }
func (realClock) After(d time.Duration) <-chan time.Time {
	if d <= 0 {
		c := make(chan time.Time, 1)
		c <- time.Now()
		return c
	}
	return time.After(d)
}

// fakeTimer 记录一个待触发的定时器。
type fakeTimer struct {
	deadline time.Time
	ch       chan time.Time
	fired    bool
}

// FakeClock 是测试用可控时钟：时间只在调用 Advance 时向前走，
// 所有到期定时器在同一次 Advance 中按截止时刻先后触发。
//
// 特殊语义：After 会额外挂一个真实时间兜底（取 deadline 与当前真实时间
// 的差值）。这样“某段代码等待 timeout”即便测试忘记 Advance 也不会永久挂起，
// 保证失败可复现；而空闲回收这类需要精确控制时刻的行为仍完全由 Advance 决定。
type FakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
	// realFallback=true 时，After 额外挂真实时间兜底；默认关闭以保证
	// 后台回收等行为完全由 Advance 决定。
	realFallback bool
}

// NewFakeClock 创建一个从 epoch 开始的假时钟。
func NewFakeClock(start time.Time) *FakeClock {
	if start.IsZero() {
		start = time.Unix(0, 0)
	}
	return &FakeClock{now: start}
}

// WithRealFallback 开启真实时间兜底并返回自身，便于链式构造。
func (c *FakeClock) WithRealFallback() *FakeClock {
	c.mu.Lock()
	c.realFallback = true
	c.mu.Unlock()
	return c
}

func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *FakeClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	ch := make(chan time.Time, 1)
	t := &fakeTimer{deadline: c.now.Add(d), ch: ch}
	c.timers = append(c.timers, t)
	fireAt := t.deadline
	fallback := c.realFallback
	c.mu.Unlock()

	// 真实时间兜底（可选）：仅用于超时等待路径，避免测试漏调 Advance 永久挂起。
	if fallback {
		time.AfterFunc(d, func() {
			c.mu.Lock()
			if t.fired {
				c.mu.Unlock()
				return
			}
			t.fired = true
			for i, x := range c.timers {
				if x == t {
					c.timers = append(c.timers[:i], c.timers[i+1:]...)
					break
				}
			}
			c.mu.Unlock()
			ch <- fireAt
		})
	}
	return ch
}

// fakeAfter 是严格假时钟版 After（永不使用真实时间兜底），供后台回收等
// 需要完全确定性的内部循环使用。
func (c *FakeClock) fakeAfter(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan time.Time, 1)
	t := &fakeTimer{deadline: c.now.Add(d), ch: ch}
	c.timers = append(c.timers, t)
	return ch
}

// Advance 把时钟向前推进 d，并触发所有已到期（含等于新时刻）的定时器。
// 触发动作在释放锁后进行，避免回调与时钟方法互相等待。
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	remaining := make([]*fakeTimer, 0, len(c.timers))
	var due []*fakeTimer
	for _, t := range c.timers {
		if !t.fired && !t.deadline.After(c.now) {
			t.fired = true
			due = append(due, t)
		}
		if !t.fired {
			remaining = append(remaining, t)
		}
	}
	c.timers = remaining
	c.mu.Unlock()

	fireAt := c.Now()
	for _, t := range due {
		t.ch <- fireAt
	}
}
