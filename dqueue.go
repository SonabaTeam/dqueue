package dqueue

import (
	"container/heap"
	"container/list"
	"runtime"
	"sync"
	"time"
)

type task struct {
	fn    func()
	runAt time.Time
}

type taskHeap []*task

func (h taskHeap) Len() int {
	return len(h)
}
func (h taskHeap) Less(i, j int) bool {
	return h[i].runAt.Before(h[j].runAt)
}
func (h taskHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}
func (h *taskHeap) Push(x any) {
	*h = append(*h, x.(*task))
}
func (h *taskHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

var (
	mu        sync.Mutex
	cond      *sync.Cond
	execQueue *list.List
	delayHeap *taskHeap
	running   bool
	wakeup    chan struct{}
	wg        sync.WaitGroup
)

func init() {
	execQueue = list.New()
	delayHeap = &taskHeap{}
	heap.Init(delayHeap)
	cond = sync.NewCond(&mu)
	wakeup = make(chan struct{}, 1)
}

func Start() {
	mu.Lock()
	if running {
		mu.Unlock()
		return
	}
	running = true
	mu.Unlock()
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
}

func scheduler() {
	defer wg.Done()
	timer := time.NewTimer(time.Hour)
	for {
		mu.Lock()
		if !running {
			mu.Unlock()
			return
		}
		if delayHeap.Len() == 0 {
			mu.Unlock()
			<-wakeup
			continue
		}
		now := time.Now()
		t := (*delayHeap)[0]
		if t.runAt.After(now) {
			wait := time.Until(t.runAt)
			timer.Reset(wait)
			mu.Unlock()
			select {
			case <-timer.C:
			case <-wakeup:
				timer.Stop()
			}
			continue
		}
		heap.Pop(delayHeap)
		execQueue.PushBack(t.fn)
		cond.Signal()
		mu.Unlock()
	}
}

func worker() {
	defer wg.Done()
	for {
		mu.Lock()
		for execQueue.Len() == 0 && running {
			cond.Wait()
		}
		if !running && execQueue.Len() == 0 {
			mu.Unlock()
			return
		}
		element := execQueue.Front()
		if element == nil {
			mu.Unlock()
			continue
		}
		fn := element.Value.(func())
		execQueue.Remove(element)
		mu.Unlock()
		if fn != nil {
			fn()
		}
	}
}

func Push(fn func(), delay time.Duration) {
	mu.Lock()
	defer mu.Unlock()
	if !running {
		return
	}
	heap.Push(delayHeap, &task{fn: fn, runAt: time.Now().Add(delay)})
	select {
	case wakeup <- struct{}{}:
	default:
	}
}

func PushFront(fn func()) {
	mu.Lock()
	defer mu.Unlock()
	if !running {
		return
	}
	execQueue.PushFront(fn)
	cond.Signal()
}
