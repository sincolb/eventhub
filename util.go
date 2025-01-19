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
		fmt.Errorf("failed to convert event data of type %T to target type %T for event: %v", data, result, name)
}
