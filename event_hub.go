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
	if len(size) > 0 {
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
	if hub.ringSize() >= size {
		hub.mu.RLock()
		last := hub.list.Latest().(*eventPayload)
		if data, ok := last.Payload(); ok {
			hub.mu.RUnlock()
			return data, nil
		}
		hub.mu.RUnlock()
	}

	cond, subscribe := hub.condSubscriber(size, timeout)
	defer hub.UnSubscribe(cond)

	data, err := subscribe(ctx, func(down *downError) ([]any, error) {
		if last := hub.list.Latest(); last != nil {
			if data, ok := last.(*eventPayload).Payload(); ok {
				return []any{data}, nil
			}
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

func (hub *EventHub) Subscribes(timeout time.Duration, size int) ([]any, error) {
	return hub.SubscribesWithContext(context.Background(), timeout, size)
}

func (hub *EventHub) SubscribesWithContext(ctx context.Context, timeout time.Duration,
	size int) ([]any, error) {
	if hub.Closed() {
		return nil, ErrEventHubClosed
	}

	if size <= 0 || size > hub.capacity {
		size = hub.capacity
	}

	if hub.ringSize() >= size {
		hub.mu.RLock()
		defer hub.mu.RUnlock()
		elements := hub.list.Take(size)
		list := make([]any, len(elements))
		for i := 0; i < len(elements); i++ {
			list[i] = elements[i].(*eventPayload).payload
		}
		return list, nil
	}

	cond, subscribe := hub.condSubscriber(size, timeout)
	defer hub.UnSubscribe(cond)

	return subscribe(ctx, func(_ *downError) ([]any, error) {
		elements := hub.list.Take(size)
		list := make([]any, len(elements))
		for i := 0; i < len(elements); i++ {
			list[i] = elements[i].(*eventPayload).payload
		}
		return list, nil
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
	defer hub.closeSubscribers()

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

	return nil
}

func (hub *EventHub) Close() {
	if !hub.closed.CompareAndSwap(0, 1) {
		return
	}

	hub.mu.Lock()
	hub.closeSubscribers()
	hub.subscribers.Clear()
	// hub.list = nil
	close(hub.done)
	hub.mu.Unlock()
}

func (hub *EventHub) Closed() bool {
	return hub.closed.Load() == 1
}

type ReducerFunc func(*downError) ([]any, error)

func (hub *EventHub) condSubscriber(size int, timeout time.Duration) (
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
			for !hub.Closed() && !canceled.Load() && hub.ringSize() < size {
				cond.Wait()
			}
		}()

		select {
		case <-ready:
			hub.mu.RLock()
			defer hub.mu.RUnlock()
			if hub.list == nil {
				return nil, nil
			}
			return reducer(canceled)
		case <-timer.C:
			return down(ErrEventHubTimeout)
		case <-hub.done:
			return down(ErrEventHubClosed)
		case <-ctx.Done():
			return down(ctx.Err())
		}
	}
}

func (hub *EventHub) ringSize() int {
	hub.mu.RLock()
	defer hub.mu.RUnlock()

	if hub.list != nil {
		return hub.list.Len()
	}
	return 0
}

func (hub *EventHub) down(cond *sync.Cond) (*downError, func(error) ([]any, error)) {
	canceled := new(downError)

	return canceled, func(err error) ([]any, error) {
		canceled.setError(err)
		cond.Signal()
		return nil, err
	}
}

func (hub *EventHub) closeSubscribers() {
	hub.subscribers.Range(func(key, value any) bool {
		if cond, ok := key.(*sync.Cond); ok {
			cond.Signal()
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
