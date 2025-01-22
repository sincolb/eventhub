package eventhub

import (
	"fmt"
	"sync"
	"time"
)

func ExampleEventHubTable_Subscribe() {
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

func ExampleEventHubTable_Subscribes() {
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
	// [4 3 2] <nil>
}
