package eventhub

import (
	"fmt"
	"sync"
	"time"
)

func ExampleSubscirbe() {
	table := NewEventHubTable[string]()
	defer table.Stop()

	const eventName = "name"
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 5; i++ {
			table.Distribute(eventName, 0, i)
		}
	}()
	wg.Wait()
	res, err := table.Subscribe(eventName, time.Millisecond*100)
	fmt.Println(res, err)
	// Output:
	// 4 <nil>
}

func ExampleSubscirbes() {
	table := NewEventHubTable[string]()
	defer table.Stop()

	const eventName = "name"
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 5; i++ {
			table.Distribute(eventName, 0, i)
		}
	}()
	wg.Wait()
	res, err := table.Subscribes(eventName, time.Millisecond*100, 3)
	fmt.Println(res, err)
	// Output:
	// [2 1 0] <nil>
}
