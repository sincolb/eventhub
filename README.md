# eventhub
A stand-alone version of the event subscription component

```
eventHubTable := eventhub.NewEventHubTable[string]()
defer eventHubTable.Stop()

done := make(chan struct{})
var wg sync.WaitGroup
wg.Add(1)
go func() {
	done <- struct{}{}
	eventHubTable.Distribute("eventNname", time.Second*1, "event data")
}()
<-done
got, err := eventHubTable.Subscribe("eventNname", time.Millisecond*100)
fmt.Println(got, err)
```
OR
```
hub := eventhub.NewEventHub()
defer hub.Close()
hub.Publish(&struct {
	Name string
}{
	Name: "title",
})
got, err = hub.Subscribe(time.Millisecond * 100)
fmt.Println(got, err)
```
