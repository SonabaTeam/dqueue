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
	mu     sync.Mutex
	cond   = sync.NewCond(&mu)
	heapQ  taskHeap
	ready  chan *task
	wakeup chan struct{}

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
	wakeup = make(chan struct{}, 1)
	seqCounter = 0
	mu.Unlock()
	wg.Add(1)
	go normalizeSeq()
	wg.Add(1)
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
	select {
	case wakeup <- struct{}{}:
	default:
	}
	mu.Unlock()
	wg.Wait()
	mu.Lock()
	heapQ = nil
	close(ready)
	mu.Unlock()
}

func normalizeSeq() {
	defer wg.Done()
	ticker := time.NewTicker(6 * time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			mu.Lock()
			if len(heapQ) == 0 {
				seqCounter = 0
				mu.Unlock()
				continue
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
			mu.Unlock()
		case <-time.After(1 * time.Second):
			mu.Lock()
			if !running {
				mu.Unlock()
				return
			}
			mu.Unlock()
		}
	}
}

func scheduler() {
	defer wg.Done()
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	for {
		mu.Lock()
		for len(heapQ) == 0 && running {
			cond.Wait()
		}
		if !running {
			mu.Unlock()
			return
		}
		now := time.Now()
		t := heapQ[0]
		if t.runAt.After(now) {
			wait := time.Until(t.runAt)
			timer.Reset(wait)
			mu.Unlock()
			select {
			case <-timer.C:
			case <-wakeup:
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
			}
			continue
		}
		heap.Pop(&heapQ)
		mu.Unlock()
		select {
		case ready <- t:
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func worker() {
	defer wg.Done()
	for task := range ready {
		if task != nil && task.fn != nil {
			task.fn()
		}
	}
}

func Push(fn func(), delay time.Duration) {
	mu.Lock()
	if !running {
		mu.Unlock()
		return
	}
	seqCounter++
	heap.Push(&heapQ, &task{
		fn:    fn,
		runAt: time.Now().Add(delay),
		seq:   seqCounter,
	})
	select {
	case wakeup <- struct{}{}:
	default:
	}
	cond.Signal()
	mu.Unlock()
}

func PushFront(fn func()) {
	mu.Lock()
	if !running {
		mu.Unlock()
		return
	}
	seqCounter--
	heap.Push(&heapQ, &task{
		fn:    fn,
		seq:   seqCounter,
		runAt: time.Now(),
	})
	select {
	case wakeup <- struct{}{}:
	default:
	}
	cond.Signal()
	mu.Unlock()
}
