package dqueue

import (
	"container/heap"
	"sync"
	"time"
)

type task struct {
	fn    func()
	runAt time.Time
	index int
}

type taskHeap []*task

func (h taskHeap) Len() int { return len(h) }

func (h taskHeap) Less(i, j int) bool {
	return h[i].runAt.Before(h[j].runAt)
}

func (h taskHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *taskHeap) Push(x any) {
	item := x.(*task)
	item.index = len(*h)
	*h = append(*h, item)
}

func (h *taskHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*h = old[:n-1]
	return item
}

var (
	mu    sync.Mutex
	cond  = sync.NewCond(&mu)
	heapQ taskHeap
	ready chan *task

	running bool
	wg      sync.WaitGroup

	workers = 4
)

func Start() {
	mu.Lock()
	if running {
		mu.Unlock()
		return
	}
	running = true
	heap.Init(&heapQ)
	ready = make(chan *task, 1024)
	mu.Unlock()
	wg.Add(1)
	go scheduler()
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go worker()
	}
}

func Stop() {
	mu.Lock()
	if !running {
		mu.Unlock()
		return
	}
	running = false
	cond.Broadcast()
	heapQ = nil
	close(ready)
	mu.Unlock()
	wg.Wait()
}

func scheduler() {
	defer wg.Done()
	for {
		mu.Lock()
		for len(heapQ) == 0 && running {
			cond.Wait()
		}
		if !running {
			mu.Unlock()
			return
		}
		task := heapQ[0]
		now := time.Now()
		if task.runAt.After(now) {
			wait := time.Until(task.runAt)
			mu.Unlock()
			time.Sleep(wait)
			continue
		}
		heap.Pop(&heapQ)
		mu.Unlock()
		ready <- task
	}
}

func worker() {
	defer wg.Done()
	for task := range ready {
		task.fn()
	}
}

func Push(fn func(), delay time.Duration) {
	mu.Lock()
	defer mu.Unlock()
	if !running {
		running = true
		heap.Init(&heapQ)
	}
	heap.Push(&heapQ, &task{
		fn:    fn,
		runAt: time.Now().Add(delay),
	})
	cond.Signal()
}

func PushFront(fn func()) {
	mu.Lock()
	defer mu.Unlock()
	heap.Push(&heapQ, &task{
		fn:    fn,
		runAt: time.Now(),
	})
	cond.Signal()
}
