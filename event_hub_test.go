package eventhub

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSubscirbs(t *testing.T) {
	runCheckedTest(t, func(t *testing.T) {
		hub := NewEventHub(5)
		defer hub.Close()

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 5; i++ {
				hub.Publish(i, 0)
			}
		}()
		wg.Wait()
		res, err := hub.Subscribes(time.Millisecond*100, 3)
		require.NoError(t, err)
		require.ElementsMatch(t, []int{4, 3, 2}, res)
		res, err = hub.Subscribes(time.Millisecond*100, 5)
		require.NoError(t, err)
		require.ElementsMatch(t, []int{4, 3, 2, 1, 0}, res)
	})
}

func TestSubscirbsTimeout(t *testing.T) {
	runCheckedTest(t, func(t *testing.T) {
		hub := NewEventHub()
		defer hub.Close()

		var wg sync.WaitGroup
		wg.Add(3)
		go func() {
			defer wg.Done()
			res, err := hub.Subscribes(0, 1)
			require.Equal(t, ErrEventHubTimeout, err)
			require.Nil(t, res)
		}()
		go func() {
			defer wg.Done()
			res, err := hub.Subscribes(time.Millisecond*10, 3)
			require.Equal(t, ErrEventHubTimeout, err)
			require.Nil(t, res)
		}()
		go func() {
			defer wg.Done()
			res, err := hub.Subscribes(time.Millisecond*20, 3)
			require.Equal(t, ErrEventHubTimeout, err)
			require.Nil(t, res)
		}()
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

		res, err := hub.SubscribesWithContext(ctx, time.Millisecond*100, 11)
		assert.Equal(t, context.Canceled, err)
		assert.Nil(t, res)
		wg.Wait()
	})
}

func TestSubscirbsClose(t *testing.T) {
	runCheckedTest(t, func(t *testing.T) {
		t.Run("close immediately", func(t *testing.T) {
			t.Parallel()
			hub := NewEventHub()
			defer hub.Close()
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				// close immediately
				hub.Close()
				hub.Publish(1)
			}()

			res, err := hub.Subscribe(time.Millisecond * 100)
			assert.Equal(t, ErrEventHubClosed, err)
			assert.Nil(t, res)
			res, err = hub.Subscribes(time.Millisecond*100, 11)
			assert.Equal(t, ErrEventHubClosed, err)
			assert.Nil(t, res)
			res, err = hub.SubscribeWithContext(context.Background(), 0)
			assert.Equal(t, ErrEventHubClosed, err)
			assert.Nil(t, res)
			res, err = hub.SubscribesWithContext(context.Background(), 0, 1)
			assert.Equal(t, ErrEventHubClosed, err)
			assert.Nil(t, res)
			wg.Wait()
		})
		t.Run("close at running ", func(t *testing.T) {
			t.Parallel()
			const num = 1024
			hub := NewEventHub(5)
			defer hub.Close()
			var wg sync.WaitGroup
			for i := 0; i < num; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					hub.Publish(i)
				}(i)
			}
			var total int32
			for i := 0; i < num; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					hub.SubscribesWithContext(context.Background(), time.Millisecond*100, 1)
					atomic.AddInt32(&total, 1)
				}()
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				time.Sleep(time.Millisecond * 5)
				hub.Close()
			}()
			wg.Wait()
			assert.Equal(t, int32(num), total)
		})
	})
}
