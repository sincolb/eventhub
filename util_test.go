package eventhub

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSubscribeEventWrapper(t *testing.T) {
	runCheckedTest(t, func(t *testing.T) {
		table := NewEventHubTable[string]()
		defer table.Stop()

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			table.Distribute("name", time.Second, "data")
		}()
		wg.Wait()

		res, err := SubscribeEventWrapper[string](table, "name", time.Millisecond*10)
		r := require.New(t)
		r.NoError(err)
		r.Equal("data", res)
		r.IsType("string", res)
	})
}

func TestSubscribeEventWrapperFail(t *testing.T) {
	runCheckedTest(t, func(t *testing.T) {
		table := NewEventHubTable[string]()

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			table.Distribute("name", time.Second, "data")
		}()
		wg.Wait()

		r := require.New(t)
		res, err := SubscribeEventWrapper[int](table, "name", time.Millisecond*10)
		r.Equal(0, res)
		r.Error(err)

		table.Stop()
		res, err = SubscribeEventWrapper[int](table, "name", time.Millisecond*10)
		r.Equal(ErrEventHubTableClosed, err)
		r.Equal(0, res)
	})
}
