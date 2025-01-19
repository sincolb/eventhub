# eventhub
A stand-alone version of the event subscription component

```
eventHubTable := eventhub.NewEventHubTable[string]()
defer eventHubTable.Stop()

done := make(chan struct{})
go func() {
	eventHubTable.Distribute("eventNname", time.Millisecond*10, "event data")
	done <- struct{}{}
}()
<-done
got, err := eventHubTable.Subscribe("eventNname", time.Millisecond*100)
fmt.Println(got, err)
```
OR
```
hub := eventhub.NewEventHub()
defer hub.Close()

var wg sync.WaitGroup
wg.Add(1)
go func() {
	defer wg.Done()
	got, err = hub.Subscribe(time.Millisecond * 10)
	fmt.Println(got, err)

}()
hub.Publish(&struct {
	Name string
}{
	Name: "title",
})
wg.Wait()
```
