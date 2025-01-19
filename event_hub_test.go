package eventhub

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestSubscribe(t *testing.T) {
	runCheckedTest(t, func(t *testing.T) {
		eventHubTable := NewEventHubTable[string]()
		defer eventHubTable.Stop()

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

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, item := range tests {
				t.Run(item.name, func(t *testing.T) {
					eventHubTable.Distribute(item.name, time.Second, item.data)
				})
			}
		}()
		wg.Wait()
		for _, test := range tests {
			got, err := eventHubTable.Subscribe(test.name, time.Millisecond*10)
			require.NoError(t, err)
			require.Exactly(t, test.data, got)
		}
	})
}

func TestMultiSubscribe(t *testing.T) {
	runCheckedTest(t, func(t *testing.T) {
		eventHubTable := NewEventHubTable[string]()
		defer eventHubTable.Stop()

		t.Run("multiple", func(t *testing.T) {
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
			var wg sync.WaitGroup
			wg.Add(2)
			time.AfterFunc(time.Millisecond*5, func() {
				defer wg.Done()
				eventHubTable.Distribute("after", 0, "after")
			})
			time.AfterFunc(time.Millisecond*10, func() {
				defer wg.Done()
				got, err := eventHubTable.Subscribe("after", time.Millisecond*20)
				fmt.Println(got, err)
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
		eventHubTable := NewEventHubTable[string]()
		defer eventHubTable.Stop()

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			eventHubTable.Distribute("name", time.Second, "payload")
			eventHubTable.Distribute("name", time.Second, "payload new")
		}()
		wg.Wait()

		got, err := eventHubTable.Subscribe("name", time.Millisecond*10)
		assert.Nil(t, err)
		assert.Exactly(t, "payload new", got)

		t.Run("another", func(t *testing.T) {
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
