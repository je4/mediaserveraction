package actionCache

import (
	"github.com/je4/utils/v2/pkg/zLogger"
	"sync"
)

type Queue[V any] struct {
	sync.Mutex
	Items  []*V
	head   int
	tail   int
	count  int
	size   int
	out    chan V
	new    chan bool
	end    chan bool
	logger zLogger.ZLogger
}

func NewQueue[V any](size int, logger zLogger.ZLogger) *Queue[V] {
	return &Queue[V]{
		Items:  make([]*V, size),
		size:   size,
		out:    make(chan V),
		end:    make(chan bool),
		new:    make(chan bool),
		logger: logger,
	}
}

func (q *Queue[V]) SetSize(size int) {
	size = max(size, 0)
	q.Lock()
	defer q.Unlock()
	q.size = size
	if q.size != len(q.Items) {
		q.resize()
	}
	q.logger.Debug().Msgf("queue size set to %d", q.size)
}

func (q *Queue[V]) Len() int {
	q.Lock()
	defer q.Unlock()
	return q.count
}

func (q *Queue[V]) Push(item V) bool {
	q.Lock()
	defer q.Unlock()
	if q.count >= q.size {
		return false
	}
	q.Items[q.tail] = &item
	q.tail = (q.tail + 1) & (len(q.Items) - 1)
	q.count++
	q.new <- true
	return true
}

func (q *Queue[V]) Start() {
	go func() {
		for {
			item, ok := q.Pop()
			// if no item is available, wait for new items
			if !ok {
				select {
				case <-q.new:
				case <-q.end:
					return
					/*
						case <-time.After(10 * time.Second):
							continue
					*/
				}
			} else {
				// empty the new channel
			L:
				for {
					select {
					case _, ok := <-q.new:
						if !ok {
							break L
						}
					default:
						break L
					}
				}
				select {
				case q.out <- item:
				case <-q.end:
					return
				}
			}
		}
	}()
}

func (q *Queue[V]) Stop() {
	q.end <- true
}

func (q *Queue[V]) resize() {
	if q.size == len(q.Items) {
		return
	}
	newItems := make([]*V, q.size)
	if q.head < q.tail {
		copy(newItems, q.Items[q.head:q.tail])
	} else {
		n := copy(newItems, q.Items[q.head:])
		copy(newItems[n:], q.Items[:q.tail])
	}
	q.head = 0
	q.tail = q.count
	q.Items = newItems
}

func (q *Queue[V]) Out() <-chan V {
	return q.out
}

func (q *Queue[V]) Pop() (V, bool) {
	q.Lock()
	defer q.Unlock()
	if q.count <= 0 {
		return *new(V), false
	}
	ret := q.Items[q.head]
	q.Items[q.head] = nil
	// bitwise modulus
	q.head = (q.head + 1) & (len(q.Items) - 1)
	q.count--
	if q.size < len(q.Items) && q.count <= q.size {
		q.resize()
	}
	return *ret, true
}

func (q *Queue[V]) AddSize(size int) {
	q.SetSize(q.size + size)
}
