package eventhub

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

const defaultEventChanSize = 10

type EventHub struct {
	subscribers *sync.Map
	done        chan struct{}
	list        *Ring

	mu       sync.RWMutex
	closed   *atomic.Int32
	capacity int
}

func NewEventHub(size ...int) *EventHub {
	hub := &EventHub{
		subscribers: &sync.Map{},
		done:        make(chan struct{}),
		closed:      &atomic.Int32{},
	}
	hub.capacity = defaultEventChanSize
	if len(size) > 0 && size[0] > 0 {
		hub.capacity = size[0]
	}
	hub.list = NewRing(hub.capacity)
	return hub
}

func (hub *EventHub) Subscribe(timeout time.Duration) (any, error) {
	return hub.SubscribeWithContext(context.Background(), timeout)
}

func (hub *EventHub) SubscribeWithContext(ctx context.Context, timeout time.Duration) (
	any, error) {
	if hub.Closed() {
		return nil, ErrEventHubClosed
	}

	const size = 1
	if list := hub.freshList(1); len(list) > 0 {
		return list[0], nil
	}

	cond, subscribe := hub.condSubscriber(size, timeout, false)
	defer hub.UnSubscribe(cond)

	data, err := subscribe(ctx, func(down *downError) ([]any, error) {
		if list := hub.freshList(1); len(list) > 0 {
			return list, nil
		}
		if e := down.Error(); e != nil {
			return nil, e
		} else if hub.Closed() {
			return nil, ErrEventHubClosed
		} else if ctx.Err() != nil {
			return nil, ctx.Err()
		} else {
			return nil, ErrEventHubTimeout
		}
	})
	if len(data) > 0 {
		return data[0], nil
	}

	return nil, err
}

func (hub *EventHub) Subscribes(timeout time.Duration, size int, returnPartial ...bool) ([]any, error) {
	partial := len(returnPartial) > 0 && returnPartial[0]
	return hub.SubscribesWithContext(context.Background(), timeout, size, partial)
}

func (hub *EventHub) SubscribesWithContext(ctx context.Context, timeout time.Duration, size int,
	returnPartial bool) ([]any, error) {
	if hub.Closed() {
		return nil, ErrEventHubClosed
	}

	if size <= 0 || size > hub.capacity {
		size = hub.capacity
	}

	if list := hub.freshList(size); len(list) >= size {
		return list, nil
	}

	cond, subscribe := hub.condSubscriber(size, timeout, returnPartial)
	defer hub.UnSubscribe(cond)

	return subscribe(ctx, func(_ *downError) ([]any, error) {
		return hub.freshList(size), nil
	})
}

func (hub *EventHub) UnSubscribe(key any) {
	if hub.Closed() {
		return
	}

	if cond, ok := key.(*sync.Cond); ok {
		cond.L.Lock()
		cond.Signal()
		cond.L.Unlock()
	}

	hub.subscribers.Delete(key)
}

func (hub *EventHub) Publish(data any, life ...time.Duration) error {
	if hub.Closed() {
		return ErrEventHubClosed
	}

	var lifeTime time.Duration
	if len(life) > 0 {
		lifeTime = life[0]
	}

	hub.mu.Lock()
	if hub.list != nil {
		payload := &eventPayload{
			payload:  data,
			life:     lifeTime,
			lastTime: time.Now(),
		}
		hub.list.Add(payload)
	}
	hub.mu.Unlock()

	// 数据写入后再唤醒订阅者;不持有 hub.mu,避免与等待侧 cond.L -> hub.mu 的锁序形成环路
	hub.closeSubscribers()

	return nil
}

func (hub *EventHub) Close() {
	if !hub.closed.CompareAndSwap(0, 1) {
		return
	}

	close(hub.done)
	hub.closeSubscribers()
}

func (hub *EventHub) Closed() bool {
	return hub.closed.Load() == 1
}

type ReducerFunc func(*downError) ([]any, error)

func (hub *EventHub) condSubscriber(size int, timeout time.Duration, returnPartial bool) (
	*sync.Cond, func(context.Context, ReducerFunc) ([]any, error)) {
	cond := sync.NewCond(&sync.Mutex{})
	hub.subscribers.Store(cond, struct{}{})

	return cond, func(ctx context.Context, reducer ReducerFunc) ([]any, error) {
		canceled, down := hub.down(cond)
		ready := make(chan struct{})
		timer := time.NewTimer(timeout)
		defer timer.Stop()

		go func() {
			cond.L.Lock()
			defer func() {
				cond.L.Unlock()
				close(ready)
			}()
			for !hub.Closed() && !canceled.Load() && len(hub.freshList(size)) < size {
				cond.Wait()
			}
		}()

		caller := func(e error) ([]any, error) {
			_, err := down(e)
			if !returnPartial {
				return nil, err
			}
			data, _ := reducer(canceled)
			return data, err
		}
		select {
		case <-ready:
			hub.mu.RLock()
			defer hub.mu.RUnlock()
			list, rerr := reducer(canceled)
			if len(list) >= size {
				return list, nil
			}
			// 数据不足:等待通常因 Close 竞争提前终止,返回错误而非"空数据+nil",
			// 避免调用方把关闭误判为正常返回(数据过期竞态极窄,视为无终止原因)
			if hub.Closed() {
				if !returnPartial {
					return nil, ErrEventHubClosed
				}
				return list, ErrEventHubClosed
			}
			return list, rerr
		case <-timer.C:
			return caller(ErrEventHubTimeout)
		case <-hub.done:
			return caller(ErrEventHubClosed)
		case <-ctx.Done():
			return caller(ctx.Err())
		}
	}
}

// freshList 返回 ring 中最近 size 条未过期的 payload,按时间序(最旧在前)返回;
// 若未过期的不够 size 条,返回全部未过期数据。不消费数据,可重复读取。
func (hub *EventHub) freshList(size int) []any {
	list := make([]any, 0, size)
	hub.list.ForEachNewest(func(v any) bool {
		if p, ok := v.(*eventPayload); ok {
			if data, fresh := p.Payload(); fresh {
				list = append(list, data)
				if len(list) >= size {
					return false
				}
			}
		}
		return true
	})
	// 反转为时间序,与 Subscribes 的返回约定一致
	for i, j := 0, len(list)-1; i < j; i, j = i+1, j-1 {
		list[i], list[j] = list[j], list[i]
	}
	return list
}

func (hub *EventHub) down(cond *sync.Cond) (*downError, func(error) ([]any, error)) {
	canceled := new(downError)

	return canceled, func(err error) ([]any, error) {
		canceled.setError(err)
		cond.L.Lock()
		cond.Signal()
		cond.L.Unlock()
		return nil, err
	}
}

func (hub *EventHub) closeSubscribers() {
	hub.subscribers.Range(func(key, value any) bool {
		if cond, ok := key.(*sync.Cond); ok {
			// 必须在持有 cond.L 时 Signal,否则与等待侧"检查-等待"窗口竞争会丢失唤醒
			cond.L.Lock()
			cond.Signal()
			cond.L.Unlock()
		}
		return true
	})
}

type downError struct {
	canceled atomic.Bool
	onceErr  sync.Once
	lastErr  atomic.Value
}

func (d *downError) setError(e error) {
	d.onceErr.Do(func() {
		d.canceled.Store(true)
		d.lastErr.Store(e)
	})
}

func (d *downError) Load() bool {
	return d.canceled.Load()
}

func (d *downError) Error() error {
	if e := d.lastErr.Load(); e != nil {
		return e.(error)
	}
	return nil
}
