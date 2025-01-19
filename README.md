# eventhub
A stand-alone version of the event subscription component

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
got, err = hub.Subscribe(time.Millisecond * 10)
fmt.Println(got, err)
wg.Wait()
```
