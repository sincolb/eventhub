package eventhub

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

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
	return table.SubscribeWithContext(context.Background(), name, timeout, size...)
}

func (table *EventHubTable[T]) SubscribeWithContext(ctx context.Context, name T, timeout time.Duration,
	size ...int) (any, error) {
	hub, err := table.acquireHub(name, size...)
	if err != nil {
		return nil, err
	}

	return hub.SubscribeWithContext(ctx, timeout)
}

func (table *EventHubTable[T]) Subscribes(name T, timeout time.Duration, size int) ([]any, error) {
	return table.SubscribesWithContext(context.Background(), name, timeout, size)
}

func (table *EventHubTable[T]) SubscribesWithContext(ctx context.Context, name T, timeout time.Duration, size int) ([]any, error) {
	hub, err := table.acquireHub(name, size)
	if err != nil {
		return nil, err
	}

	return hub.SubscribesWithContext(ctx, timeout, size)
}

func (table *EventHubTable[T]) acquireHub(name T, size ...int) (*EventHub, error) {
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
	return hub, nil
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

	table.mu.RLock()
	hub, ok := table.subscriptions[name]
	table.mu.RUnlock()
	if ok {
		return hub.Publish(data, life)
	}

	option := buildEventHubTableOptions(opts...)
	if option.autoCommit {
		table.mu.Lock()
		if option.capacity == nil {
			table.subscriptions[name] = NewEventHub()
		} else {
			table.subscriptions[name] = NewEventHub(*option.capacity)
		}
		table.mu.Unlock()
		return table.subscriptions[name].Publish(data, life)
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
