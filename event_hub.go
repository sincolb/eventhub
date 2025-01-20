package eventhub

import (
	"context"
	"sync"
	"time"
)

const defaultEventChanSize = 10

type EventHub struct {
	subscribers map[chan any]struct{}
	eventChan   chan any
	done        chan struct{}
	cond        *sync.Cond
	list        *Ring

	once     sync.Once
	mu       sync.RWMutex
	capacity int
	closed   int32
}

func NewEventHub(size ...int) *EventHub {
	hub := &EventHub{
		subscribers: make(map[chan any]struct{}),
		done:        make(chan struct{}),
	}
	capacity := defaultEventChanSize
	if len(size) > 0 {
		capacity = size[0]
	}
	hub.capacity = capacity
	hub.eventChan = make(chan any, capacity)
	hub.list = NewRing(capacity)
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
			hub.mu.RLock()
			subscribers := hub.subscribers
			hub.mu.RUnlock()
			for ch := range subscribers {
				select {
				case ch <- data:
					hub.mu.Lock()
					delete(hub.subscribers, ch)
					hub.mu.Unlock()
					close(ch)
				case <-hub.done:
					return
				}
			}
		case <-hub.done:
			return
		}
	}
}

func (hub *EventHub) Subscribe(timeout time.Duration) (any, error) {
	return hub.SubscribeWithContext(context.Background(), timeout)
}

func (hub *EventHub) SubscribeWithContext(ctx context.Context, timeout time.Duration) (any, error) {
	if hub.Closed() {
		return nil, ErrEventHubClosed
	}

	hub.mu.RLock()
	if hub.list.Len() >= 1 {
		last := hub.list.Pop().(*eventPayload)
		if data, ok := last.Payload(); ok {
			defer hub.mu.RUnlock()
			return data, nil
		}
	}
	hub.mu.RUnlock()

	ch := make(chan any, 1)
	hub.mu.Lock()
	hub.subscribers[ch] = struct{}{}
	hub.mu.Unlock()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case v, ok := <-ch:
		if !ok {
			return nil, ErrEventHubChanClosed
		}
		return v, nil
	case <-timer.C:
		hub.UnSubscribe(ch)
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

	capacity := size
	if size > hub.capacity {
		capacity = hub.capacity
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
		for !*canceled && hub.list.Len() < capacity {
			hub.cond.Wait()
		}
	}()

	select {
	case <-ready:
		hub.mu.RLock()
		elements := hub.list.Take(size)
		hub.mu.RUnlock()
		data := make([]any, len(elements))
		for i := 0; i < len(elements); i++ {
			data[i] = elements[i].(*eventPayload).payload
		}
		return data, nil
	case <-timer.C:
		return down(ErrEventHubTimeout)
	case <-hub.done:
		return down(ErrEventHubClosed)
	case <-ctx.Done():
		return down(ctx.Err())
	}
}

func (hub *EventHub) down() (*bool, func(error) ([]any, error)) {
	canceled := false

	return &canceled, func(err error) ([]any, error) {
		defer func() {
			hub.cond.L.Lock()
			hub.cond.Signal()
			hub.cond.L.Unlock()
		}()
		canceled = true
		return nil, err
	}
}

func (hub *EventHub) UnSubscribe(ch chan any) {
	hub.mu.Lock()
	defer hub.mu.Unlock()

	delete(hub.subscribers, ch)
}

func (hub *EventHub) Publish(data any, life ...time.Duration) error {
	if hub.Closed() {
		return ErrEventHubClosed
	}

	var lifeTime time.Duration
	if len(life) > 0 {
		lifeTime = life[0]
	}

	payload := &eventPayload{
		payload:  data,
		life:     lifeTime,
		lastTime: time.Now(),
	}

	select {
	case hub.eventChan <- data:
	case <-hub.done:
		hub.drain()
		return ErrEventHubClosed
	default:
	}

	select {
	case <-hub.done:
		hub.drain()
		return ErrEventHubClosed
	default:
		hub.mu.Lock()
		hub.list.Add(payload)
		hub.mu.Unlock()

		hub.cond.L.Lock()
		hub.cond.Signal()
		hub.cond.L.Unlock()
	}

	return nil
}

func (hub *EventHub) Close() {
	hub.once.Do(func() {
		hub.closed = 1
		hub.list = nil
		hub.subscribers = make(map[chan any]struct{})
		close(hub.done)
	})
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
