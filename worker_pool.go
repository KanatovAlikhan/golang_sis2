package main

import (
	"errors"
	"fmt"
	"sync"
)

const (
	DefaultWorkerCount   = 2
	DefaultQueueCapacity = 3
)

var (
	ErrInvalidPoolConfig = errors.New("invalid worker pool configuration")
	ErrQueueFull         = errors.New("task queue is full")
	ErrPoolStopped       = errors.New("worker pool has stopped")
)


type WorkerPool struct {
	mu      sync.Mutex 
	store   *TaskStore
	queue   chan int
	stopped bool
	wg      sync.WaitGroup
}


func NewWorkerPool(store *TaskStore, workerCount, queueCapacity int) (*WorkerPool, error) {
	if store == nil || workerCount < 2 || queueCapacity < 1 {
		return nil, fmt.Errorf("%w: need a store, at least 2 workers, and a positive queue capacity", ErrInvalidPoolConfig)
	}
	pool := &WorkerPool{
		store: store,
		queue: make(chan int, queueCapacity),
	}

	pool.wg.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go pool.work()
	}
	return pool, nil
}


func (pool *WorkerPool) Submit(id int) error {
	pool.mu.Lock()
	defer pool.mu.Unlock()
	if pool.stopped {
		return ErrPoolStopped
	}

	
	pool.store.mu.Lock()
	defer pool.store.mu.Unlock()
	task, exists := pool.store.tasks[id]
	if !exists {
		return fmt.Errorf("%w: ID %d", ErrTaskNotFound, id)
	}
	if task.Status != Pending {
		return fmt.Errorf("%w: task %d is %s", ErrInvalidTransition, id, task.Status)
	}
	select {
	case pool.queue <- id:
		
		task.Status = Queued
		return nil
	default:
		return ErrQueueFull
	}
}

func (pool *WorkerPool) work() {
	defer pool.wg.Done()
	for id := range pool.queue {
		_, _ = pool.store.processTask(id, Queued)
	}
}

func (pool *WorkerPool) Shutdown() {
	pool.mu.Lock()
	if !pool.stopped {
		pool.stopped = true
		close(pool.queue)
	}
	pool.mu.Unlock()
	pool.wg.Wait()
}
