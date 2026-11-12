package eventhub

import (
	"errors"
	"fmt"
	"reflect"
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

func TestEventHubTableDistributeAutoCommit(t *testing.T) {
	table := NewEventHubTable[string]()
	defer table.Stop()

	// autoCommit:无订阅者时先写入历史,后续订阅可重复读取
	if err := table.Distribute("k", 0, "v1"); err != nil {
		t.Fatalf("distribute: %v", err)
	}
	if err := table.Distribute("k", 0, "v2"); err != nil {
		t.Fatalf("distribute: %v", err)
	}

	for i := 0; i < 2; i++ {
		got, err := table.Subscribe("k", 200*time.Millisecond)
		if err != nil {
			t.Fatalf("subscribe #%d: %v", i, err)
		}
		if got != "v2" {
			t.Fatalf("subscribe #%d = %v, want v2", i, got)
		}
	}
}

func TestEventHubTableDistributeNoAutoCommit(t *testing.T) {
	table := NewEventHubTable[string]()
	defer table.Stop()

	err := table.Distribute("k", 0, "v", WithEventHubTableAutoCommit(false))
	if !errors.Is(err, ErrEventHubNotSubscribed) {
		t.Fatalf("err = %v, want ErrEventHubNotSubscribed", err)
	}
}

func TestEventHubTableUnSubscribe(t *testing.T) {
	table := NewEventHubTable[string]()
	defer table.Stop()

	if _, err := table.Subscribe("k", 20*time.Millisecond); !errors.Is(err, ErrEventHubTimeout) {
		t.Fatalf("err = %v, want ErrEventHubTimeout", err)
	}

	table.UnSubscribe("k")

	// hub 已被移除:非 autoCommit 分发应报未订阅
	err := table.Distribute("k", 0, "v", WithEventHubTableAutoCommit(false))
	if !errors.Is(err, ErrEventHubNotSubscribed) {
		t.Fatalf("err = %v, want ErrEventHubNotSubscribed", err)
	}
}

func TestEventHubTableStop(t *testing.T) {
	table := NewEventHubTable[string]()
	_ = table.Distribute("k", 0, "v")
	table.Stop()

	if _, err := table.Subscribe("k", 10*time.Millisecond); !errors.Is(err, ErrEventHubTableClosed) {
		t.Fatalf("subscribe after stop: err = %v, want ErrEventHubTableClosed", err)
	}
	if err := table.Distribute("k", 0, "v"); !errors.Is(err, ErrEventHubTableClosed) {
		t.Fatalf("distribute after stop: err = %v, want ErrEventHubTableClosed", err)
	}
}

func TestEventHubTableStopUnblocksSubscriber(t *testing.T) {
	table := NewEventHubTable[string]()

	done := make(chan error, 1)
	go func() {
		_, err := table.Subscribe("k", 5*time.Second)
		done <- err
	}()

	time.Sleep(30 * time.Millisecond)
	table.Stop()

	select {
	case err := <-done:
		if !errors.Is(err, ErrEventHubClosed) {
			t.Fatalf("err = %v, want ErrEventHubClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("subscriber was not woken up by table.Stop")
	}
}

func TestEventHubTableConcurrentStop(t *testing.T) {
	table := NewEventHubTable[string]()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("k%d", id)
			for j := 0; j < 20; j++ {
				_ = table.Distribute(key, 0, j)
				_, _ = table.Subscribe(key, time.Millisecond)
				if j%5 == 0 {
					table.UnSubscribe(key)
				}
			}
		}(i)
	}

	time.Sleep(5 * time.Millisecond)
	table.Stop()
	wg.Wait()

	if _, err := table.Subscribe("k", time.Millisecond); !errors.Is(err, ErrEventHubTableClosed) {
		t.Fatalf("subscribe after stop: err = %v, want ErrEventHubTableClosed", err)
	}
}

func TestSubscribesEventWrapper(t *testing.T) {
	table := NewEventHubTable[string]()
	defer table.Stop()

	if err := table.Distribute("k", 0, "a"); err != nil {
		t.Fatal(err)
	}
	if err := table.Distribute("k", 0, "b"); err != nil {
		t.Fatal(err)
	}

	got, err := SubscribesEventWrapper[string](table, "k", 200*time.Millisecond, 2, false)
	if err != nil {
		t.Fatalf("wrapper: %v", err)
	}
	if !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Fatalf("got %v, want [a b]", got)
	}
}

// TestSubscribesSizeNonPositive 首次订阅 size<=0 不应 panic(回归:NewRing(0) 曾 panic
// 且 panic 时持有 table.mu 导致后续 Stop 死锁)
func TestSubscribesSizeNonPositive(t *testing.T) {
	table := NewEventHubTable[string]()

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Subscribes(size<=0) panicked: %v", r)
		}
	}()

	_, err := table.Subscribes("k", 10*time.Millisecond, 0, false)
	if !errors.Is(err, ErrEventHubTimeout) {
		t.Fatalf("err = %v, want ErrEventHubTimeout", err)
	}

	// 锁应已被正常释放:Stop 需在超时前完成
	stopped := make(chan struct{})
	go func() {
		table.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop() hung: table mutex not released")
	}
}

// TestSubscribesEventWrapperPartialTypeMismatch 部分返回时类型不匹配应返回错误
// 而不是 panic(回归:recover 未覆盖错误分支)
func TestSubscribesEventWrapperPartialTypeMismatch(t *testing.T) {
	table := NewEventHubTable[string]()
	defer table.Stop()

	if err := table.Distribute("k", 0, 42); err != nil {
		t.Fatal(err)
	}

	_, err := SubscribesEventWrapper[string](table, "k", 10*time.Millisecond, 3, true)
	if err == nil {
		t.Fatal("expected conversion error, got nil")
	}
}

// TestEventHubTableDistributeCapacityOption 分发时自定义容量生效,非法容量回退默认值
func TestEventHubTableDistributeCapacityOption(t *testing.T) {
	table := NewEventHubTable[string]()
	defer table.Stop()

	if err := table.Distribute("k", 0, "v", WithEventHubCapacity(2)); err != nil {
		t.Fatalf("distribute: %v", err)
	}
	if hub := table.subscriptions["k"]; hub == nil || hub.capacity != 2 {
		t.Fatalf("hub = %v, want capacity 2", hub)
	}

	if err := table.Distribute("k2", 0, "v", WithEventHubCapacity(0)); err != nil {
		t.Fatalf("distribute: %v", err)
	}
	if got := table.subscriptions["k2"].capacity; got != defaultEventChanSize {
		t.Fatalf("invalid capacity fallback = %d, want %d", got, defaultEventChanSize)
	}
}

// TestEventHubTableSubscribeWithZeroSize 变长参 size=0 经 acquireHub 创建合法容量,不 panic
func TestEventHubTableSubscribeWithZeroSize(t *testing.T) {
	table := NewEventHubTable[string]()
	defer table.Stop()

	if _, err := table.Subscribe("k", 20*time.Millisecond, 0); !errors.Is(err, ErrEventHubTimeout) {
		t.Fatalf("err = %v, want ErrEventHubTimeout", err)
	}
}
