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
	eventChan   chan any
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
	hub.eventChan = make(chan any, hub.capacity)
	hub.list = NewRing(hub.capacity)
	go hub.start()
	return hub
}

func (hub *EventHub) start() {
	for {
		select {
		case data, ok := <-hub.eventChan:
			if !ok {
				return
			}
			var exit bool
			hub.subscribers.Range(func(key, _ any) bool {
				ch, ok := key.(chan any)
				if !ok {
					return true
				}
				select {
				case ch <- data:
					hub.subscribers.Delete(ch)
					close(ch)
				case <-hub.done:
					exit = true
					return false
				}
				return true
			})
			if exit {
				return
			}
		case <-hub.done:
			return
		}
	}

}

func (hub *EventHub) Subscribe(timeout time.Duration) (any, error) {
	return hub.SubscribeWithContext(context.Background(), timeout)
}

func (hub *EventHub) SubscribeWithContext(ctx context.Context, timeout time.Duration) (
	any, error) {
	if hub.Closed() {
		return nil, ErrEventHubClosed
	}

	if hub.list.Len() >= 1 {
		hub.mu.RLock()
		last := hub.list.Latest().(*eventPayload)
		if data, ok := last.Payload(); ok {
			defer hub.mu.RUnlock()
			return data, nil
		}
		hub.mu.RUnlock()
	}

	ch := make(chan any, 1)
	hub.subscribers.Store(ch, struct{}{})
	defer hub.UnSubscribe(ch)

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case v, ok := <-ch:
		if !ok {
			return nil, ErrEventHubChanClosed
		}
		return v, nil
	case <-timer.C:
		return nil, ErrEventHubTimeout
	case <-hub.done:
		return nil, ErrEventHubClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
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

	if hub.ringSize() == size {
		hub.mu.RLock()
		defer hub.mu.RUnlock()
		elements := hub.list.Take(size)
		list := make([]any, len(elements))
		for i := 0; i < len(elements); i++ {
			list[i] = elements[i].(*eventPayload).payload
		}
		return list, nil
	}

	cond := sync.NewCond(&sync.Mutex{})
	hub.subscribers.Store(cond, struct{}{})
	defer hub.UnSubscribe(cond)

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
		if hub.list == nil {
			return nil, nil
		}
		elements := hub.list.Take(size)
		hub.mu.RUnlock()
		list := make([]any, len(elements))
		for i := 0; i < len(elements); i++ {
			list[i] = elements[i].(*eventPayload).payload
		}
		return list, nil
	case <-timer.C:
		return down(ErrEventHubTimeout)
	case <-hub.done:
		return down(ErrEventHubClosed)
	case <-ctx.Done():
		return down(ctx.Err())
	}
}

func (hub *EventHub) UnSubscribe(key any) {

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

	select {
	case hub.eventChan <- data:
	case <-hub.done:
		close(hub.eventChan)
		return ErrEventHubClosed
	default:
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

func (hub *EventHub) ringSize() int {
	hub.mu.RLock()
	defer hub.mu.RUnlock()

	if hub.list != nil {
		return hub.list.Len()
	}
	return 0
}

func (hub *EventHub) down(cond *sync.Cond) (*atomic.Bool, func(error) ([]any, error)) {
	canceled := &atomic.Bool{}
	canceled.Store(false)

	return canceled, func(err error) ([]any, error) {
		canceled.Store(true)
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
