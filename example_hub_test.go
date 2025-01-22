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
	// [2 1 0] <nil>
}
