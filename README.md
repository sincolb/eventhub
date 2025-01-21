# eventhub

<div align=center>

[![codecov](https://codecov.io/github/sincolb/eventhub/graph/badge.svg?token=VDA3VL01X8)](https://codecov.io/github/sincolb/eventhub)
[![GoDoc](https://godoc.org/github.com/sincolb/eventhub?status.svg)](https://godoc.org/github.com/sincolb/eventhub)
[![Go Report Card](https://goreportcard.com/badge/github.com/sincolb/eventhub)](https://goreportcard.com/report/github.com/sincolb/eventhub)
[![Workflow for Codecov Action](https://github.com/sincolb/eventhub/actions/workflows/go.yml/badge.svg)](https://github.com/sincolb/eventhub/actions/workflows/go.yml)

</div>

`eventhub` is a stand-alone version of the event subscription component that can accept multiple or one results, and supports timeouts and contexts.

## Install
```console
go get github.com/sincolb/eventhub
```
## Usage
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
### Subscribe only once
```
hub := eventhub.NewEventHub()
defer hub.Close()

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
```
### Subscribes for multiple
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
