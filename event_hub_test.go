package eventhub

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSubscirbs(t *testing.T) {
	runCheckedTest(t, func(t *testing.T) {
		hub := NewEventHub()
		defer hub.Close()

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 5; i++ {
				hub.Publish(i, 0)
				// simulates latency into the eventhub
				time.Sleep(time.Millisecond * 10)
			}
		}()
		res, err := hub.Subscribes(time.Millisecond*100, 4)
		require.NoError(t, err)
		require.ElementsMatch(t, []int{3, 2, 1, 0}, res)
		wg.Wait()
	})
}

func TestSubscirbsTimeout(t *testing.T) {
	runCheckedTest(t, func(t *testing.T) {
		hub := NewEventHub()
		defer hub.Close()

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 5; i++ {
				hub.Publish(i, 0)
				// simulates latency into the eventhub
				time.Sleep(time.Millisecond * 100)
			}
		}()
		res, err := hub.Subscribes(time.Millisecond*100, 4)
		assert.Equal(t, err, ErrEventHubTimeout)
		assert.Nil(t, res)
		wg.Wait()
	})
}

func TestSubscirbsContextCancel(t *testing.T) {
	runCheckedTest(t, func(t *testing.T) {
		hub := NewEventHub()
		defer hub.Close()

		ctx, cancel := context.WithCancel(context.Background())
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 5; i++ {
				hub.Publish(i, 0)
				// simulates latency into the eventhub
				time.Sleep(time.Millisecond * 10)
				cancel()
			}
		}()

		res, err := hub.SubscribesWithContext(ctx, time.Millisecond*100, 4)
		assert.Equal(t, err, context.Canceled)
		assert.Nil(t, res)
		wg.Wait()
	})
}
