package eventhub

import (
	"fmt"
	"sync"
)

// A Ring can be used as fixed size ring.
type Ring struct {
	elements []any
	index    int
	lock     sync.RWMutex
}

// NewRing returns a Ring object with the given size n.
func NewRing(n int) *Ring {
	if n < 1 {
		panic("n should be greater than 0")
	}

	return &Ring{
		elements: make([]any, n),
	}
}

// Add adds v into r.
func (r *Ring) Add(v any) {
	r.lock.Lock()
	defer r.lock.Unlock()

	rlen := len(r.elements)
	r.elements[r.index%rlen] = v
	r.index++

	// prevent ring index overflow
	if r.index >= rlen<<1 {
		r.index -= rlen
	}
}

// Take takes all items from r.
func (r *Ring) Take(n ...int) []any {
	r.lock.RLock()
	defer r.lock.RUnlock()

	var size int
	var start int
	rlen := len(r.elements)

	if r.index > rlen {
		size = rlen
		start = r.index % rlen
	} else {
		size = r.index
	}

	num := size
	if len(n) > 0 && n[0] > 0 && n[0] <= size {
		num = n[0]
	}
	elements := make([]any, num)
	switch num {
	case 1:
		fmt.Println(111)
		elements[0] = r.elements[(start+size-1)%rlen]
	case size:
		fmt.Println("size=", size)
		for i := 0; i < size; i++ {
			elements[i] = r.elements[(start+i)%rlen]
		}
	default:
		fmt.Println("num=", num)
		j := 0
		for i := num - 1; i >= 0; i-- {
			elements[j] = r.elements[(start+i)%rlen]
			j++
		}
	}
	fmt.Println("elements=", elements)
	return elements
}

func (r *Ring) Pop() any {
	if elements := r.Take(1); len(elements) > 0 {
		return elements[0]
	}

	return nil
}

// Take get size from r.
func (r *Ring) Len() int {
	r.lock.RLock()
	defer r.lock.RUnlock()

	size, rlen := 0, len(r.elements)
	if r.index > rlen {
		size = rlen
	} else {
		size = r.index
	}

	return size
}
