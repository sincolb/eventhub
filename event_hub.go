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
	cond        *sync.Cond
	list        *Ring

	mu       sync.RWMutex
	capacity int
	closed   int32
}

func NewEventHub(size ...int) *EventHub {
	hub := &EventHub{
		subscribers: &sync.Map{},
		done:        make(chan struct{}),
	}
	hub.capacity = defaultEventChanSize
	if len(size) > 0 {
		hub.capacity = size[0]
	}
	hub.eventChan = make(chan any, hub.capacity)
	hub.list = NewRing(hub.capacity)
	hub.cond = sync.NewCond(&sync.Mutex{})
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
			hub.subscribers.Range(func(key any, value any) bool {
				ch := key.(chan any)
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

	hub.mu.RLock()
	if hub.list.Len() >= 1 {
		last := hub.list.Latest().(*eventPayload)
		if data, ok := last.Payload(); ok {
			defer hub.mu.RUnlock()
			return data, nil
		}
	}
	hub.mu.RUnlock()

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
	canceled, down := hub.down()
	ready := make(chan struct{})
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	go func() {
		hub.cond.L.Lock()
		defer func() {
			hub.cond.L.Unlock()
			close(ready)
		}()
		for !canceled.Load() && hub.list.Len() < size {
			hub.cond.Wait()
		}
	}()

	select {
	case <-ready:
		hub.mu.RLock()
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

func (hub *EventHub) UnSubscribe(ch chan any) {

	hub.subscribers.Delete(ch)
}

func (hub *EventHub) Publish(data any, life ...time.Duration) error {
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
		hub.drain()
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
	hub.cond.L.Lock()
	hub.cond.Signal()
	hub.cond.L.Unlock()

	return nil
}

func (hub *EventHub) Close() {
	if hub.Closed() {
		return
	}

	hub.mu.Lock()
	hub.subscribers.Clear()
	hub.closed = 1
	hub.list = nil
	close(hub.done)
	hub.mu.Unlock()
}

func (hub *EventHub) Closed() bool {
	hub.mu.RLock()
	defer hub.mu.RUnlock()

	return hub.closed == 1
}

func (hub *EventHub) drain() {
	close(hub.eventChan)
	for range hub.eventChan {
	}
}

func (hub *EventHub) down() (*atomic.Bool, func(error) ([]any, error)) {
	canceled := &atomic.Bool{}
	canceled.Store(false)

	return canceled, func(err error) ([]any, error) {
		defer func() {
			hub.cond.L.Lock()
			hub.cond.Signal()
			hub.cond.L.Unlock()
		}()
		canceled.Store(true)
		return nil, err
	}
}
