package eventhub

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestSubscribe(t *testing.T) {
	type Placeholder struct{}
	tests := []struct {
		name string
		data any
	}{
		{
			name: "a",
			data: "payload",
		},
		{
			name: "b",
			data: Placeholder{},
		},
		{
			name: "c",
			data: &Placeholder{},
		},
		{
			name: "d",
			data: []any{1, 2, 3},
		},
		{
			name: "e",
			data: map[string]struct{}{},
		},
		{
			name: "f",
			data: make(chan struct{}),
		},
	}
	runCheckedTest(t, func(t *testing.T) {
		for _, item := range tests {
			item := item
			t.Run(item.name, func(t *testing.T) {
				t.Parallel()
				table := NewEventHubTable[string]()
				defer table.Stop()
				go func() {
					table.Distribute(item.name, time.Second, item.data)
				}()
				got, err := table.Subscribe(item.name, time.Millisecond*10)
				if err == nil {
					assert.NoError(t, err)
					assert.Exactly(t, item.data, got)
				} else {
					assert.Equal(t, ErrEventHubTimeout, err)
					assert.Nil(t, got)
				}
			})
		}
	})
}

func TestMultiSubscribe(t *testing.T) {
	runCheckedTest(t, func(t *testing.T) {

		t.Run("multiple", func(t *testing.T) {
			t.Parallel()
			eventHubTable := NewEventHubTable[string]()
			defer eventHubTable.Stop()
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				eventHubTable.Distribute("name", time.Second, "payload")
			}()
			wg.Wait()

			got, err := eventHubTable.Subscribe("name", time.Millisecond*10)
			assert.Nil(t, err)
			assert.Exactly(t, "payload", got)

			got, err = eventHubTable.Subscribe("name", time.Millisecond*10)
			assert.Nil(t, err)
			assert.Equal(t, "payload", got)
		})

		t.Run("another", func(t *testing.T) {
			t.Parallel()
			eventHubTable := NewEventHubTable[string]()
			defer eventHubTable.Stop()
			var wg sync.WaitGroup
			wg.Add(2)
			time.AfterFunc(time.Millisecond*5, func() {
				defer wg.Done()
				eventHubTable.Distribute("after", 0, "after")
			})
			time.AfterFunc(time.Millisecond*10, func() {
				defer wg.Done()
				got, err := eventHubTable.Subscribe("after", time.Millisecond*20)
				assert.Nil(t, err)
				assert.Equal(t, "after", got)
			})
			got, err := eventHubTable.Subscribe("after", time.Millisecond*20)
			assert.Nil(t, err)
			assert.Equal(t, "after", got)
			wg.Wait()
		})
	})
}

func TestDistribute(t *testing.T) {
	runCheckedTest(t, func(t *testing.T) {
		eventHubTable := NewEventHubTable[string]()
		defer eventHubTable.Stop()

		err := eventHubTable.Distribute("name", time.Second, nil, WithEventHubTableAutoCommit(false))
		assert.Equal(t, ErrEventHubNotSubscribed, err)

		err = eventHubTable.Distribute("name", time.Second, nil)
		assert.Nil(t, err)
	})
}

func TestMultiDistribute(t *testing.T) {
	runCheckedTest(t, func(t *testing.T) {
		t.Run("basic", func(t *testing.T) {
			t.Parallel()
			eventHubTable := NewEventHubTable[string]()
			defer eventHubTable.Stop()

			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				eventHubTable.Distribute("name", time.Second, "payload", WithEventHubCapacity(10))
				eventHubTable.Distribute("name", time.Second, "payload new")
			}()
			wg.Wait()

			got, err := eventHubTable.Subscribe("name", time.Millisecond*10)
			assert.Nil(t, err)
			assert.Exactly(t, "payload new", got)
		})

		t.Run("another", func(t *testing.T) {
			t.Parallel()
			eventHubTable := NewEventHubTable[string]()
			defer eventHubTable.Stop()
			var wg sync.WaitGroup
			wg.Add(2)
			time.AfterFunc(time.Millisecond*2, func() {
				defer wg.Done()
				err := eventHubTable.Distribute("another", 0, "another")
				require.NoError(t, err)
			})
			time.AfterFunc(time.Millisecond*4, func() {
				defer wg.Done()
				err := eventHubTable.Distribute("another", 0, "another data")
				require.NoError(t, err)
			})
			wg.Wait()
			got, err := eventHubTable.Subscribe("another", time.Millisecond*10)
			assert.Nil(t, err)
			assert.Equal(t, "another data", got)

		})
	})
}

func TestUnubscribe(t *testing.T) {
	runCheckedTest(t, func(t *testing.T) {
		eventHubTable := NewEventHubTable[string]()
		defer eventHubTable.Stop()

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			eventHubTable.Distribute("name", time.Second, "payload")
		}()
		wg.Wait()

		eventHubTable.UnSubscribe("name")

		got, err := eventHubTable.Subscribe("name", time.Millisecond*10)
		assert.Nil(t, got)
		assert.Equal(t, ErrEventHubTimeout, err)
	})
}

func TestTimeout(t *testing.T) {
	runCheckedTest(t, func(t *testing.T) {
		eventHubTable := NewEventHubTable[string]()
		defer eventHubTable.Stop()

		got, err := eventHubTable.Subscribe("name", time.Millisecond*10)
		assert.Nil(t, got)
		assert.Equal(t, ErrEventHubTimeout, err)
		// assert.Error(t, err)
		// assert.EqualError(t, err, "eventHub: timeout")

		got, err = eventHubTable.Subscribe("name", time.Millisecond*5)
		assert.Nil(t, got)
		assert.Equal(t, ErrEventHubTimeout, err)

		eventHubTable.Distribute("name", time.Millisecond*5, nil)
		time.Sleep(time.Millisecond * 10)
		got, err = eventHubTable.Subscribe("name", time.Millisecond*10)
		assert.Nil(t, got)
		assert.Equal(t, ErrEventHubTimeout, err)
	})
}

func TestStop(t *testing.T) {
	runCheckedTest(t, func(t *testing.T) {
		eventHubTable := NewEventHubTable[string]()
		defer eventHubTable.Stop()

		got, err := eventHubTable.Subscribe("name", time.Millisecond*10)
		assert.Nil(t, got)
		assert.Equal(t, ErrEventHubTimeout, err)
		// assert.Error(t, err)
		// assert.EqualError(t, err, "eventHub: timeout")
	})
}

func TestClose(t *testing.T) {
	runCheckedTest(t, func(t *testing.T) {
		eventHubTable := NewEventHubTable[string]()
		// stop immediately
		eventHubTable.Stop()
		eventHubTable.Stop()
		eventHubTable.UnSubscribe("name")

		err := eventHubTable.Distribute("name", 0, nil)
		assert.Equal(t, ErrEventHubTableClosed, err)
		res, err := eventHubTable.Subscribe("name", time.Millisecond*100)
		assert.Equal(t, ErrEventHubTableClosed, err)
		assert.Nil(t, res)
		res, err = eventHubTable.Subscribes("name", time.Millisecond*100, 11)
		assert.Equal(t, ErrEventHubTableClosed, err)
		assert.Nil(t, res)
	})
}

func TestDistributeLifeTime(t *testing.T) {
	runCheckedTest(t, func(t *testing.T) {
		eventHubTable := NewEventHubTable[string]()
		defer eventHubTable.Stop()

		err := eventHubTable.Distribute("name", time.Millisecond*10, "payload")
		assert.Nil(t, err)

		got, err := eventHubTable.Subscribe("name", time.Millisecond*10)
		assert.Nil(t, err)
		assert.Equal(t, "payload", got)

		time.Sleep(time.Millisecond * 10)
		got, err = eventHubTable.Subscribe("name", time.Millisecond*10)
		assert.Nil(t, got)
		assert.Equal(t, ErrEventHubTimeout, err)
	})
}

func TestEventTableSubscirbs(t *testing.T) {
	runCheckedTest(t, func(t *testing.T) {
		eventHubTable := NewEventHubTable[string]()
		defer eventHubTable.Stop()

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 5; i++ {
				eventHubTable.Distribute("name", 0, i)
				// simulates latency into the eventhub
				time.Sleep(time.Millisecond * 10)
			}
		}()
		res, err := eventHubTable.Subscribes("name", time.Millisecond*200, 4)
		if err == nil {
			require.Len(t, res, 4)
		} else {
			require.Equal(t, ErrEventHubTimeout, err)
		}
		wg.Wait()
	})
}

func BenchmarkSubscribe(b *testing.B) {
	b.ReportAllocs()
	eventHubTable := NewEventHubTable[string]()
	defer eventHubTable.Stop()

	eventHubTable.Distribute("name", 0, "data")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := eventHubTable.Subscribe("name", 0); err != nil {
			b.Fatal("subscribed should success")
		}
	}
}

func runCheckedTest(t *testing.T, fn func(t *testing.T)) {
	defer goleak.VerifyNone(t)
	fn(t)
}
