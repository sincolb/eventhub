package eventhub

import (
	"reflect"
	"testing"
)

func TestRingAddLenLatest(t *testing.T) {
	ring := NewRing(3)
	if got := ring.Len(); got != 0 {
		t.Fatalf("empty ring len = %d, want 0", got)
	}
	if got := ring.Latest(); got != nil {
		t.Fatalf("empty ring latest = %v, want nil", got)
	}

	ring.Add("a")
	ring.Add("b")
	if got := ring.Len(); got != 2 {
		t.Fatalf("len = %d, want 2", got)
	}
	if got := ring.Latest(); got != "b" {
		t.Fatalf("latest = %v, want b", got)
	}

	ring.Add("c")
	ring.Add("d") // 覆盖最旧的 a
	if got := ring.Len(); got != 3 {
		t.Fatalf("len = %d, want 3", got)
	}
	if got := ring.Latest(); got != "d" {
		t.Fatalf("latest = %v, want d", got)
	}
	if got := ring.Take(); !reflect.DeepEqual(got, []any{"b", "c", "d"}) {
		t.Fatalf("take all = %v, want [b c d]", got)
	}
}

func TestRingTakeSingle(t *testing.T) {
	ring := NewRing(4)
	ring.Add(1)
	ring.Add(2)
	ring.Add(3)

	if got := ring.Take(1); !reflect.DeepEqual(got, []any{3}) {
		t.Fatalf("take(1) = %v, want [3]", got)
	}
	// 重复读取:不消费历史
	if got := ring.Take(1); !reflect.DeepEqual(got, []any{3}) {
		t.Fatalf("take(1) again = %v, want [3]", got)
	}
}

// TestRingTakePartial 固化 Take(n) 部分取的现有行为(团队确认保持不变)
func TestRingTakePartial(t *testing.T) {
	ring := NewRing(4)
	ring.Add("a")
	ring.Add("b")
	ring.Add("c")

	// 未满环:size=3、start=0、num=2
	if got := ring.Take(2); !reflect.DeepEqual(got, []any{"b", "a"}) {
		t.Fatalf("take(2) on partially filled ring = %v, want [b a]", got)
	}

	ring.Add("d")
	ring.Add("e") // 满环,最旧元素为 b

	// 满环:size=4、start=1、num=3
	if got := ring.Take(3); !reflect.DeepEqual(got, []any{"d", "c", "b"}) {
		t.Fatalf("take(3) on full ring = %v, want [d c b]", got)
	}
}

// TestRingForEachNewest 验证 ForEachNewest 从最新到最旧的遍历顺序及提前终止
func TestRingForEachNewest(t *testing.T) {
	// 空环:不调用回调
	{
		ring := NewRing(3)
		calls := 0
		ring.ForEachNewest(func(v any) bool {
			calls++
			return true
		})
		if calls != 0 {
			t.Fatalf("empty ring calls = %d, want 0", calls)
		}
	}

	// 未满环:从最新到最旧
	{
		ring := NewRing(4)
		ring.Add("a")
		ring.Add("b")
		ring.Add("c")

		var got []any
		ring.ForEachNewest(func(v any) bool {
			got = append(got, v)
			return true
		})
		if !reflect.DeepEqual(got, []any{"c", "b", "a"}) {
			t.Fatalf("partial ring order = %v, want [c b a]", got)
		}
	}

	// 满环回绕:index=5、start=1,最新为 e
	{
		ring := NewRing(4)
		for _, v := range []string{"a", "b", "c", "d", "e"} {
			ring.Add(v)
		}

		var got []any
		ring.ForEachNewest(func(v any) bool {
			got = append(got, v)
			return true
		})
		if !reflect.DeepEqual(got, []any{"e", "d", "c", "b"}) {
			t.Fatalf("full ring order = %v, want [e d c b]", got)
		}
	}

	// 提前终止:fn 返回 false 后不再遍历
	{
		ring := NewRing(4)
		for _, v := range []string{"a", "b", "c", "d"} {
			ring.Add(v)
		}

		var got []any
		ring.ForEachNewest(func(v any) bool {
			got = append(got, v)
			return len(got) < 2
		})
		if !reflect.DeepEqual(got, []any{"d", "c"}) {
			t.Fatalf("early stop = %v, want [d c]", got)
		}
	}
}

// TestRingTakeEdgeCases 空环上各种取数参数均返回空且不越界
func TestRingTakeEdgeCases(t *testing.T) {
	ring := NewRing(3)

	if got := ring.Take(); len(got) != 0 {
		t.Fatalf("take() on empty ring = %v, want empty", got)
	}
	for _, n := range []int{0, -1, 5} {
		if got := ring.Take(n); len(got) != 0 {
			t.Fatalf("take(%d) on empty ring = %v, want empty", n, got)
		}
	}
}
