package dqueue

import (
	"container/heap"
	"runtime"
	"sync"
	"time"
)

type task struct {
	fn    func()
	runAt time.Time
	seq   int
	index int
}

type taskHeap []*task

func (h taskHeap) Len() int { return len(h) }

func (h taskHeap) Less(i, j int) bool {
	if h[i].runAt.Equal(h[j].runAt) {
		return h[i].seq < h[j].seq
	}
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

	seqCounter int
	running    bool
	wg         sync.WaitGroup
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
	seqCounter = 0
	mu.Unlock()
	wg.Add(1)
	go normalizeSeq()
	go scheduler()
	for i := 0; i < runtime.NumCPU(); i++ {
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

func normalizeSeq() {
	ticker := time.NewTicker(6 * time.Hour)
	defer ticker.Stop()
	for range ticker.C {
		mu.Lock()
		func() {
			if len(heapQ) == 0 {
				seqCounter = 0
				return
			}
			minSeq := heapQ[0].seq
			for _, t := range heapQ {
				if t.seq < minSeq {
					minSeq = t.seq
				}
			}
			for _, t := range heapQ {
				t.seq -= minSeq
			}
			seqCounter -= minSeq
		}()
		mu.Unlock()
	}
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
	seqCounter++
	heap.Push(&heapQ, &task{
		fn:    fn,
		runAt: time.Now().Add(delay),
		seq:   seqCounter,
	})
	cond.Signal()
}

func PushFront(fn func()) {
	mu.Lock()
	defer mu.Unlock()
	if !running {
		running = true
		heap.Init(&heapQ)
	}
	seqCounter--
	heap.Push(&heapQ, &task{
		fn:    fn,
		seq:   seqCounter,
		runAt: time.Now(),
	})
	cond.Signal()
}
