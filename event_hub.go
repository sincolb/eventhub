package eventhub

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const defaultEventChanSize = 10

var (
	ErrEventHubTableClosed   = errors.New("eventHubTable: closed")
	ErrEventHubNotSubscribed = errors.New("eventHubTable: not subscribed")

	ErrEventHubClosed     = errors.New("eventHub: closed")
	ErrEventHubTimeout    = errors.New("eventHub: timeout")
	ErrEventHubChanClosed = errors.New("eventHubChan: closed")
)

func SubscribeEventWrapper[T any, K comparable](
	table *EventHubTable[K],
	name K,
	timeout time.Duration,
	size ...int) (result T, err error) {
	data, err := table.Subscribe(name, timeout, size...)
	if err != nil {
		return result, err
	}

	if result, ok := data.(T); ok {
		return result, nil
	}

	return result,
		fmt.Errorf("failed to convert event data of type %T to target type %T for event: %v", data, result, name)
}

type EventHubTable[T comparable] struct {
	subscriptions map[T]*EventHub
	mu            sync.RWMutex
	_closed       atomic.Bool
}

func NewEventHubTable[T comparable]() *EventHubTable[T] {
	return &EventHubTable[T]{
		subscriptions: make(map[T]*EventHub),
	}
}

func (table *EventHubTable[T]) Subscribe(name T, timeout time.Duration,
	size ...int) (any, error) {
	if table.closed() {
		return nil, ErrEventHubTableClosed
	}

	table.mu.RLock()
	hub, ok := table.subscriptions[name]
	table.mu.RUnlock()

	if !ok {
		table.mu.Lock()
		hub, ok = table.subscriptions[name]
		if !ok {
			hub = NewEventHub(size...)
			table.subscriptions[name] = hub
		}
		table.mu.Unlock()
	}

	return hub.Subscribe(timeout)
}

func (table *EventHubTable[T]) UnSubscribe(name T) {
	if table.closed() {
		return
	}

	table.mu.Lock()
	defer table.mu.Unlock()

	if hub, ok := table.subscriptions[name]; ok {
		hub.Close()
		delete(table.subscriptions, name)
	}
}

func (table *EventHubTable[T]) Distribute(name T, life time.Duration, data any,
	opts ...eventHubTableOption) error {
	if table.closed() {
		return ErrEventHubChanClosed
	}

	payload := &eventPayload{
		payload:  data,
		life:     life,
		lastTime: time.Now(),
	}
	table.mu.RLock()
	hub, ok := table.subscriptions[name]
	table.mu.RUnlock()
	if ok {
		return hub.Publish(payload)
	}

	option := buildEventHubTableOptions(opts...)
	if option.autoCommit {
		table.mu.Lock()
		table.subscriptions[name] = NewEventHub()
		table.mu.Unlock()
		return table.subscriptions[name].Publish(payload)
	}

	return ErrEventHubNotSubscribed
}

func (table *EventHubTable[T]) Stop() {
	if !table._closed.CompareAndSwap(false, true) {
		return
	}

	table.mu.Lock()
	defer table.mu.Unlock()
	for _, hub := range table.subscriptions {
		hub.Close()
	}
	table.subscriptions = make(map[T]*EventHub)
}

func (table *EventHubTable[T]) closed() bool {

	return table._closed.Load()
}

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

type eventPayload struct {
	payload  any
	lastTime time.Time
	life     time.Duration
}

func (p *eventPayload) Payload() (any, bool) {
	if p.payload != nil && (p.life == 0 ||
		p.lastTime.After(time.Now().Add(-p.life))) {
		return p.payload, true
	}
	return nil, false
}

type (
	eventHubTableOptions struct {
		autoCommit bool
	}

	eventHubTableOption func(opts *eventHubTableOptions)
)

func buildEventHubTableOptions(opts ...eventHubTableOption) *eventHubTableOptions {
	options := &eventHubTableOptions{
		autoCommit: true,
	}
	for _, opt := range opts {
		opt(options)
	}

	return options
}

func WithEventHubTableAutoCommit(auto bool) eventHubTableOption {
	return func(opts *eventHubTableOptions) {
		opts.autoCommit = auto
	}
}
