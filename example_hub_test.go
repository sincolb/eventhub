package eventhub

import (
	"fmt"
	"sync"
	"time"
)

func ExampleEventHub_Subscribe() {
	hub := NewEventHub()
	defer hub.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 5; i++ {
			hub.Publish(i)
		}
	}()
	wg.Wait()
	res, err := hub.Subscribe(time.Millisecond * 100)
	fmt.Println(res, err)
	// Output:
	// 4 <nil>
}

func ExampleEventHub_Subscribes() {
	hub := NewEventHub()
	defer hub.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 5; i++ {
			hub.Publish(i)
		}
	}()
	wg.Wait()
	res, err := hub.Subscribes(time.Millisecond*100, 3)
	fmt.Println(res, err)
	// Output:
	// [2 3 4] <nil>
}

// 环形缓冲:满后覆盖最旧元素,读取不消费
func ExampleRing() {
	ring := NewRing(3)
	ring.Add(1)
	ring.Add(2)
	ring.Add(3)
	ring.Add(4) // 覆盖最旧的 1

	fmt.Println(ring.Len())
	fmt.Println(ring.Latest())
	fmt.Println(ring.Take())

	// Output:
	// 3
	// 4
	// [2 3 4]
}

// 带类型转换的订阅包装:类型不匹配时返回错误
func ExampleSubscribeEventWrapper() {
	table := NewEventHubTable[string]()
	defer table.Stop()

	_ = table.Distribute("signal", 0, "confirm")

	value, err := SubscribeEventWrapper[string](table, "signal", 100*time.Millisecond)
	fmt.Println(value, err)

	// Output:
	// confirm <nil>
}
