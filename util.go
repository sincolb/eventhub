package eventhub

import (
	"fmt"
	"time"
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
		fmt.Errorf("failed to convert event data of type %T to target type %T for event: %v",
			data, result, name)
}

func SubscribesEventWrapper[T any, K comparable](
	table *EventHubTable[K],
	name K,
	timeout time.Duration,
	size int,
	returnPartial bool) (result []T, err error) {
	defer func() {
		if e := recover(); e != nil {
			err = fmt.Errorf("failed to convert event data to target type %T for event %v: %v",
				result, name, e)
		}
	}()

	data, err := table.Subscribes(name, timeout, size, returnPartial)
	if err != nil {
		if returnPartial && len(data) > 0 {
			result = make([]T, len(data))
			for i, v := range data {
				result[i] = v.(T)
			}
		}
		return result, err
	}

	result = make([]T, len(data))
	for i, v := range data {
		result[i] = v.(T)
	}
	return
}
