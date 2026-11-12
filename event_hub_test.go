package eventhub

import (
	"context"
	"errors"
	"fmt"
	"reflect"
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
			// simulates latency into the eventhub
			time.Sleep(time.Millisecond * 10)
			cancel()
		}()

		res, err := hub.SubscribeWithContext(ctx, time.Millisecond*100)
		assert.Equal(t, context.Canceled, err)
		assert.Nil(t, res)

		res, err = hub.SubscribesWithContext(ctx, time.Millisecond*100, 11, false)
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
			res, err = hub.SubscribesWithContext(context.Background(), 0, 1, false)
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
					hub.SubscribesWithContext(context.Background(), time.Millisecond*100, 1, false)
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

func TestEventHubPublishSubscribeLatestRepeatable(t *testing.T) {
	hub := NewEventHub()
	defer hub.Close()

	if err := hub.Publish("v1"); err != nil {
		t.Fatalf("publish v1: %v", err)
	}
	if err := hub.Publish("v2"); err != nil {
		t.Fatalf("publish v2: %v", err)
	}

	// 历史可重复读取:多次订阅都返回最新一条,且不消费
	for i := 0; i < 3; i++ {
		got, err := hub.Subscribe(200 * time.Millisecond)
		if err != nil {
			t.Fatalf("subscribe #%d: %v", i, err)
		}
		if got != "v2" {
			t.Fatalf("subscribe #%d = %v, want v2", i, got)
		}
	}
}

func TestEventHubSubscribeWaitsForPublish(t *testing.T) {
	hub := NewEventHub()
	defer hub.Close()

	go func() {
		time.Sleep(30 * time.Millisecond)
		_ = hub.Publish("late")
	}()

	got, err := hub.Subscribe(time.Second)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if got != "late" {
		t.Fatalf("got %v, want late", got)
	}
}

func TestEventHubSubscribeTimeout(t *testing.T) {
	hub := NewEventHub()
	defer hub.Close()

	if _, err := hub.Subscribe(30 * time.Millisecond); !errors.Is(err, ErrEventHubTimeout) {
		t.Fatalf("err = %v, want ErrEventHubTimeout", err)
	}
}

func TestEventHubSubscribeExpiredPayload(t *testing.T) {
	hub := NewEventHub()
	defer hub.Close()

	if err := hub.Publish("stale", 20*time.Millisecond); err != nil {
		t.Fatalf("publish: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	// 过期历史不可读
	if _, err := hub.Subscribe(100 * time.Millisecond); !errors.Is(err, ErrEventHubTimeout) {
		t.Fatalf("err = %v, want ErrEventHubTimeout", err)
	}
}

func TestEventHubSubscribeUnblockedByClose(t *testing.T) {
	hub := NewEventHub()

	done := make(chan error, 1)
	go func() {
		_, err := hub.Subscribe(5 * time.Second)
		done <- err
	}()

	time.Sleep(30 * time.Millisecond)
	hub.Close()

	select {
	case err := <-done:
		if !errors.Is(err, ErrEventHubClosed) {
			t.Fatalf("err = %v, want ErrEventHubClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("subscriber was not woken up by Close")
	}
}

// TestEventHubPublishAfterClose Publish 在 Close 之后必须返回错误而不是 panic
func TestEventHubPublishAfterClose(t *testing.T) {
	hub := NewEventHub()
	hub.Close()

	for i := 0; i < 3; i++ {
		if err := hub.Publish("after-close"); !errors.Is(err, ErrEventHubClosed) {
			t.Fatalf("publish after close #%d: err = %v, want ErrEventHubClosed", i, err)
		}
	}
	if _, err := hub.Subscribe(time.Millisecond); !errors.Is(err, ErrEventHubClosed) {
		t.Fatalf("subscribe after close: err = %v, want ErrEventHubClosed", err)
	}
	if _, err := hub.Subscribes(time.Millisecond, 1, false); !errors.Is(err, ErrEventHubClosed) {
		t.Fatalf("subscribes after close: err = %v, want ErrEventHubClosed", err)
	}
}

func TestEventHubSubscribesBatch(t *testing.T) {
	hub := NewEventHub(8)
	defer hub.Close()

	if err := hub.Publish("a"); err != nil {
		t.Fatal(err)
	}
	if err := hub.Publish("b"); err != nil {
		t.Fatal(err)
	}

	// 请求数量与历史条数一致:返回全部历史,按时间序
	data, err := hub.Subscribes(200*time.Millisecond, 2, false)
	if err != nil {
		t.Fatalf("subscribes: %v", err)
	}
	if !reflect.DeepEqual(data, []any{"a", "b"}) {
		t.Fatalf("subscribes = %v, want [a b]", data)
	}

	// 只取最新一条
	data, err = hub.Subscribes(200*time.Millisecond, 1, false)
	if err != nil {
		t.Fatalf("subscribes size=1: %v", err)
	}
	if !reflect.DeepEqual(data, []any{"b"}) {
		t.Fatalf("subscribes size=1 = %v, want [b]", data)
	}
}

func TestEventHubSubscribesTimeoutPartial(t *testing.T) {
	hub := NewEventHub(8)
	defer hub.Close()

	if _, err := hub.Subscribes(30*time.Millisecond, 1, false); !errors.Is(err, ErrEventHubTimeout) {
		t.Fatalf("err = %v, want ErrEventHubTimeout", err)
	}

	if err := hub.Publish("only"); err != nil {
		t.Fatal(err)
	}
	// 请求 3 条但只有 1 条:returnPartial 返回已就绪数据 + 超时错误
	data, err := hub.Subscribes(30*time.Millisecond, 3, true)
	if !errors.Is(err, ErrEventHubTimeout) {
		t.Fatalf("err = %v, want ErrEventHubTimeout", err)
	}
	if len(data) != 1 || data[0] != "only" {
		t.Fatalf("partial data = %v, want [only]", data)
	}
}

// TestEventHubConcurrentAccess Close/Publish/Subscribe 并发下不 panic、不悬挂(配合 -race)
func TestEventHubConcurrentAccess(t *testing.T) {
	hub := NewEventHub()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				_, _ = hub.Subscribe(10 * time.Millisecond)
			}
		}()
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = hub.Publish(fmt.Sprintf("p-%d-%d", id, j))
			}
		}(i)
	}

	time.Sleep(20 * time.Millisecond)
	hub.Close()
	wg.Wait()

	if err := hub.Publish("after"); !errors.Is(err, ErrEventHubClosed) {
		t.Fatalf("publish after close: err = %v, want ErrEventHubClosed", err)
	}
}

// TestEventHubNoLostWakeup 验证订阅者在 Publish 前后任意时刻开始等待都能收到事件
func TestEventHubNoLostWakeup(t *testing.T) {
	const (
		rounds      = 20
		subscribers = 4
	)
	for round := 0; round < rounds; round++ {
		hub := NewEventHub()
		var wg sync.WaitGroup
		received := make(chan any, subscribers)
		for i := 0; i < subscribers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if v, err := hub.Subscribe(500 * time.Millisecond); err == nil {
					received <- v
				}
			}()
		}
		time.Sleep(time.Millisecond)
		_ = hub.Publish(round)
		wg.Wait()
		hub.Close()

		if got := len(received); got != subscribers {
			t.Fatalf("round %d: %d/%d subscribers received the event", round, got, subscribers)
		}
	}
}

// TestSubscribeWaitsAfterExpiry life 过期后 Subscribe 应等待新的 publish,
// 而不是瞬间返回超时(回归:过期 payload 曾让后续订阅永远无法等到新数据)
func TestSubscribeWaitsAfterExpiry(t *testing.T) {
	hub := NewEventHub()
	defer hub.Close()

	if err := hub.Publish("stale", 15*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(40 * time.Millisecond) // 等它过期

	go func() {
		time.Sleep(50 * time.Millisecond)
		_ = hub.Publish("fresh")
	}()

	start := time.Now()
	got, err := hub.Subscribe(2 * time.Second)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if got != "fresh" {
		t.Fatalf("got %v, want fresh", got)
	}
	if elapsed := time.Since(start); elapsed < 40*time.Millisecond {
		t.Fatalf("subscribe returned too early (%v), did not wait for fresh publish", elapsed)
	}
}

// TestSubscribesSkipsExpired life 过期后 Subscribes 不应返回过期数据(回归)
func TestSubscribesSkipsExpired(t *testing.T) {
	hub := NewEventHub(8)
	defer hub.Close()

	if err := hub.Publish("stale", 15*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(40 * time.Millisecond) // 等它过期

	go func() {
		time.Sleep(50 * time.Millisecond)
		_ = hub.Publish("fresh")
	}()

	data, err := hub.Subscribes(2*time.Second, 1, false)
	if err != nil {
		t.Fatalf("subscribes: %v", err)
	}
	if len(data) != 1 || data[0] != "fresh" {
		t.Fatalf("data = %v, want [fresh]", data)
	}
}

// TestEventHubSubscribeSkipsExpiredNewer 最新一条过期时回退到仍新鲜的更早历史
func TestEventHubSubscribeSkipsExpiredNewer(t *testing.T) {
	hub := NewEventHub()
	defer hub.Close()

	if err := hub.Publish("fresh-older", 10*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := hub.Publish("expired-newer", 10*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond) // 等最新一条过期

	got, err := hub.Subscribe(200 * time.Millisecond)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if got != "fresh-older" {
		t.Fatalf("got %v, want fresh-older", got)
	}
}

// TestEventHubSubscribesFiltersExpiredMix 新鲜与过期混合时,partial 只返回新鲜数据
func TestEventHubSubscribesFiltersExpiredMix(t *testing.T) {
	hub := NewEventHub(8)
	defer hub.Close()

	if err := hub.Publish("keep", 0); err != nil {
		t.Fatal(err)
	}
	if err := hub.Publish("expired", 10*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond) // 等最后一条过期

	// 请求 2 条但只有 1 条新鲜:超时后 partial 仅返回新鲜数据
	data, err := hub.Subscribes(50*time.Millisecond, 2, true)
	if !errors.Is(err, ErrEventHubTimeout) {
		t.Fatalf("err = %v, want ErrEventHubTimeout", err)
	}
	if !reflect.DeepEqual(data, []any{"keep"}) {
		t.Fatalf("data = %v, want [keep]", data)
	}
}

// TestNewEventHubCapacityGuard 非法容量回退默认值,不得 panic
func TestNewEventHubCapacityGuard(t *testing.T) {
	for _, size := range []int{0, -1} {
		hub := NewEventHub(size)
		if hub.capacity != defaultEventChanSize {
			t.Fatalf("NewEventHub(%d) capacity = %d, want %d", size, hub.capacity, defaultEventChanSize)
		}
		hub.Close()
	}

	hub := NewEventHub(2)
	if hub.capacity != 2 {
		t.Fatalf("NewEventHub(2) capacity = %d, want 2", hub.capacity)
	}
	hub.Close()
}

// TestEventHubSubscribesUnblockedByClose Subscribes 等待期间 Close 必须返回
// ErrEventHubClosed,而非"空数据 + nil"(回归:ready 路径与 Close 竞争)
func TestEventHubSubscribesUnblockedByClose(t *testing.T) {
	hub := NewEventHub()

	done := make(chan error, 1)
	go func() {
		_, err := hub.Subscribes(5*time.Second, 1, false)
		done <- err
	}()

	time.Sleep(30 * time.Millisecond)
	hub.Close()

	select {
	case err := <-done:
		if !errors.Is(err, ErrEventHubClosed) {
			t.Fatalf("err = %v, want ErrEventHubClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("subscriber was not woken up by Close")
	}
}

// TestEventHubSubscribesPartialUnblockedByClose returnPartial 时 Close 返回
// 已就绪数据 + ErrEventHubClosed
func TestEventHubSubscribesPartialUnblockedByClose(t *testing.T) {
	hub := NewEventHub()

	if err := hub.Publish("keep", 10*time.Second); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := hub.Subscribes(5*time.Second, 2, true)
		done <- err
	}()

	time.Sleep(30 * time.Millisecond)
	hub.Close()

	select {
	case err := <-done:
		if !errors.Is(err, ErrEventHubClosed) {
			t.Fatalf("err = %v, want ErrEventHubClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("subscriber was not woken up by Close")
	}
}

// TestSubscribesSizeClampedToCapacity size<=0 或超容量时钳制为 capacity(回归:size=0 曾 panic)
func TestSubscribesSizeClampedToCapacity(t *testing.T) {
	hub := NewEventHub(4)
	defer hub.Close()

	for i := 0; i < 4; i++ {
		if err := hub.Publish(i); err != nil {
			t.Fatal(err)
		}
	}

	// size<=0 与超容量均钳制为 capacity=4:数据足够,直接返回且不 panic
	for _, size := range []int{0, -1, 100} {
		data, err := hub.Subscribes(50*time.Millisecond, size, false)
		if err != nil {
			t.Fatalf("subscribes size=%d: %v", size, err)
		}
		if len(data) != 4 {
			t.Fatalf("subscribes size=%d = %d items, want 4", size, len(data))
		}
	}
}
