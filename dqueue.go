package dqueue

import (
	"sync"
	"time"
)

type DQueue struct {
	fn    func()
	delay time.Duration
}

var (
	queues  []*DQueue
	mu      sync.Mutex
	cond    *sync.Cond
	running bool
	wg      sync.WaitGroup
)

func Start() {
	if running {
		return
	}
	queues = make([]*DQueue, 100)
	cond = sync.NewCond(&mu)
	mu.Lock()
	if running {
		mu.Unlock()
		return
	}
	running = true
	mu.Unlock()
	workers := 4
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				mu.Lock()
				for len(queues) == 0 && running {
					cond.Wait()
				}
				if !running && len(queues) == 0 {
					mu.Unlock()
					return
				}
				if len(queues) == 0 {
					mu.Unlock()
					continue
				}
				queue := queues[0]
				queues = queues[1:]
				mu.Unlock()
				if queue.delay > 0 {
					time.Sleep(queue.delay)
				}
				queue.fn()
			}
		}()
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
	mu.Unlock()

	wg.Wait()
}

func Push(fn func(), delay time.Duration) {
	mu.Lock()
	if !running {
		mu.Unlock()
		Start()
		mu.Lock()
	}
	queues = append(queues, &DQueue{
		fn:    fn,
		delay: delay,
	})
	mu.Unlock()
	cond.Signal()
}

func PushFront(fn func()) {
	mu.Lock()
	if !running {
		mu.Unlock()
		Start()
		mu.Lock()
	}
	task := &DQueue{
		fn:    fn,
		delay: 0,
	}
	queues = append([]*DQueue{task}, queues...)
	mu.Unlock()
	cond.Signal()
}
