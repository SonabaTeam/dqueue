package dqueue

import (
	"sync"
	"time"
)

type DTask struct {
	fn        func()
	delay     time.Duration
	runOnMain bool
}

var (
	tasks     []*DTask
	mu        sync.Mutex
	cond      *sync.Cond
	mainQueue chan func()
	running   bool
	wg        sync.WaitGroup
)

func Start() {
	tasks = []*DTask{}
	mainQueue = make(chan func(), 100)
	cond = sync.NewCond(&mu)
	mu.Lock()
	if running {
		mu.Unlock()
		return
	}
	running = true
	mu.Unlock()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			mu.Lock()
			for len(tasks) == 0 && running {
				cond.Wait()
			}
			if !running && len(tasks) == 0 {
				mu.Unlock()
				return
			}
			if len(tasks) == 0 {
				mu.Unlock()
				continue
			}
			task := tasks[0]
			tasks = tasks[1:]
			mu.Unlock()
			if task.delay > 0 {
				time.Sleep(task.delay)
			}
			if task.runOnMain {
				select {
				case mainQueue <- task.fn:
				default:
				}
			} else {
				go task.fn()
			}
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			mu.Lock()
			if !running && len(mainQueue) == 0 {
				mu.Unlock()
				return
			}
			mu.Unlock()
			select {
			case fn := <-mainQueue:
				fn()
			default:
				time.Sleep(1 * time.Millisecond)
			}
		}
	}()
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

func Push(fn func(), delay time.Duration, runOnMain bool) {
	mu.Lock()
	if !running {
		Start()
	}
	if !running {
		tasks = append(tasks, &DTask{fn: fn, delay: delay, runOnMain: runOnMain})
		mu.Unlock()
		return
	}
	tasks = append(tasks, &DTask{fn: fn, delay: delay, runOnMain: runOnMain})
	cond.Signal()
	mu.Unlock()
}

func PushFront(fn func(), runOnMain bool) {
	Push(fn, 0, runOnMain)
}

func PushBack(fn func(), delay time.Duration, runOnMain bool) {
	Push(fn, delay, runOnMain)
}
