package eventhub

import (
	"sync"
	"time"
)

const defaultEventChanSize = 10

type EventHub struct {
	subscribers map[chan any]struct{}
	eventChan   chan any
	done        chan struct{}
	data        *eventPayload

	once   sync.Once
	mu     sync.RWMutex
	closed int32
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
	hub.eventChan = make(chan any, capacity)
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
	if hub.Closed() {
		return nil, ErrEventHubClosed
	}

	hub.mu.RLock()
	if hub.data != nil {
		if data, ok := hub.data.Payload(); ok {
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
	}
}

func (hub *EventHub) UnSubscribe(ch chan any) {
	hub.mu.Lock()
	defer hub.mu.Unlock()

	delete(hub.subscribers, ch)
}

func (hub *EventHub) Publish(data *eventPayload) error {
	if hub.Closed() {
		return ErrEventHubClosed
	}

	select {
	case hub.eventChan <- data.payload:
	case <-hub.done:
		hub.drain()
		return ErrEventHubClosed
	default:
	}

	hub.mu.Lock()
	hub.data = data
	hub.mu.Unlock()
	return nil
}

func (hub *EventHub) Close() {
	hub.once.Do(func() {
		hub.closed = 1
		hub.data = nil
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
