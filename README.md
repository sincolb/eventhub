# eventhub
A stand-alone version of the event subscription component, which accepts results only once, supports timeout waits, and context

```
eventHubTable := eventhub.NewEventHubTable[string]()
defer eventHubTable.Stop()

eventHubTable.Distribute("eventNname", time.Millisecond*10, "event data")

done := make(chan struct{})
go func() {
	got, err := eventHubTable.Subscribe("eventNname", time.Millisecond*100)
	fmt.Println(got, err)
	done <- struct{}{}
}()
<-done
```
OR
```
hub := eventhub.NewEventHub()
defer hub.Close()

var wg sync.WaitGroup
wg.Add(1)
go func() {
	defer wg.Done()
	hub.Publish(&struct {
		Name string
	}{
		Name: "title",
	})
}()
got, err := hub.Subscribe(time.Millisecond * 10)
fmt.Println(got, err)
wg.Wait()
```
OR
```
hub := NewEventHub()
defer hub.Close()

go func() {
	for i := 0; i < 5; i++ {
		hub.Publish(i, 0)
		// simulates latency into the eventhub
		time.Sleep(time.Millisecond * 10)
	}
}()
res, err := hub.Subscribes(time.Millisecond*100, 4)
fmt.Println(res, err)
```
