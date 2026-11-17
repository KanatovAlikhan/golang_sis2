package main

import (
	"errors"
	"math"
	"runtime"
	"sync"
	"testing"
	"time"
)


const concurrentTestTimeout = 5 * time.Second

type processorFunc func([]float64) (Statistics, error)

func (f processorFunc) Calculate(numbers []float64) (Statistics, error) {
	return f(numbers)
}

func waitSignal(t *testing.T, signal <-chan struct{}, operation string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(concurrentTestTimeout):
		t.Fatalf("timed out waiting for %s", operation)
	}
}

func shutdownPool(t *testing.T, pool *WorkerPool) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		pool.Shutdown()
		close(done)
	}()
	waitSignal(t, done, "worker shutdown")
}

func createPoolTask(t *testing.T, store *TaskStore, numbers ...float64) Task {
	t.Helper()
	task, err := store.CreateTask(numbers)
	if err != nil {
		t.Fatal(err)
	}
	return task
}

func TestWorkerPoolRejectsInvalidConfiguration(t *testing.T) {
	store := NewTaskStore(nil)
	for _, tc := range []struct {
		name              string
		store             *TaskStore
		workers, capacity int
	}{
		{"nil store", nil, 2, 1},
		{"zero workers", store, 0, 1},
		{"one worker", store, 1, 1},
		{"negative workers", store, -1, 1},
		{"zero capacity", store, 2, 0},
		{"negative capacity", store, 2, -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool, err := NewWorkerPool(tc.store, tc.workers, tc.capacity)
			if pool != nil {
				shutdownPool(t, pool)
				t.Fatal("invalid configuration created a pool")
			}
			if !errors.Is(err, ErrInvalidPoolConfig) {
				t.Fatalf("expected ErrInvalidPoolConfig, got %v", err)
			}
		})
	}
}

func TestWorkerPoolStartsAndStopsWithoutTasks(t *testing.T) {
	store := NewTaskStore(nil)
	pool, err := NewWorkerPool(store, 2, 3)
	if err != nil {
		t.Fatal(err)
	}
	shutdownPool(t, pool)
	shutdownPool(t, pool)
	task := createPoolTask(t, store, 1)
	if err := pool.Submit(task.ID); !errors.Is(err, ErrPoolStopped) {
		t.Fatalf("expected ErrPoolStopped, got %v", err)
	}
	stored, _ := store.GetTask(task.ID)
	if stored.Status != Pending {
		t.Fatalf("rejected task changed state to %s", stored.Status)
	}
}

func TestWorkerPoolOverlapQueueLimitAndDrain(t *testing.T) {
	started := make(chan struct{}, 10)
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	var activityMu sync.Mutex
	active, peak, calls := 0, 0, 0
	processor := processorFunc(func(numbers []float64) (Statistics, error) {
		activityMu.Lock()
		active++
		calls++
		if active > peak {
			peak = active
		}
		activityMu.Unlock()
		started <- struct{}{}
		<-release
		result, err := (StatisticsProcessor{}).Calculate(numbers)
		activityMu.Lock()
		active--
		activityMu.Unlock()
		return result, err
	})
	store := NewTaskStore(processor)
	pool, err := NewWorkerPool(store, 2, 2)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		unblock()
		shutdownPool(t, pool)
	})
	tasks := make([]Task, 5)
	for i := range tasks {
		tasks[i] = createPoolTask(t, store, float64(i), float64(i+2))
	}
	
	for i := 0; i < 2; i++ {
		submitted := make(chan error, 1)
		go func(id int) { submitted <- pool.Submit(id) }(tasks[i].ID)
		select {
		case err := <-submitted:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(concurrentTestTimeout):
			t.Fatal("Submit waited for processing to finish")
		}
		waitSignal(t, started, "independent worker to enter processing")
	}
	for i := 2; i < 4; i++ {
		if err := pool.Submit(tasks[i].ID); err != nil {
			t.Fatal(err)
		}
	}
	for i, task := range tasks[:4] {
		stored, err := store.GetTask(task.ID)
		if err != nil {
			t.Fatal(err)
		}
		want := Processing
		if i >= 2 {
			want = Queued
		}
		if stored.Status != want || stored.Result != nil {
			t.Fatalf("task %d: got %+v, want %s without result", task.ID, stored, want)
		}
		if err := pool.Submit(task.ID); !errors.Is(err, ErrInvalidTransition) {
			t.Fatalf("duplicate task %d: got %v", task.ID, err)
		}
		if _, err := store.ProcessTask(task.ID); !errors.Is(err, ErrInvalidTransition) {
			t.Fatalf("synchronous processing bypassed reservation for task %d: %v", task.ID, err)
		}
	}
	if err := pool.Submit(tasks[4].ID); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("expected ErrQueueFull, got %v", err)
	}
	pending, _ := store.GetTask(tasks[4].ID)
	if pending.Status != Pending || pending.Result != nil {
		t.Fatalf("queue rejection corrupted task: %+v", pending)
	}
	
	if err := pool.Submit(tasks[2].ID); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("queued duplicate: got %v", err)
	}
	shutdownDone := make(chan struct{})
	go func() { pool.Shutdown(); close(shutdownDone) }()

	deadline := time.After(concurrentTestTimeout)
waitingForStop:
	for {
		err := pool.Submit(tasks[4].ID)
		if errors.Is(err, ErrPoolStopped) {
			break
		}
		if !errors.Is(err, ErrQueueFull) {
			t.Fatalf("expected full queue or stopped pool, got %v", err)
		}
		select {
		case <-deadline:
			t.Fatal("shutdown did not stop accepting submissions")
			break waitingForStop
		default:
			runtime.Gosched()
		}
	}
	select {
	case <-shutdownDone:
		t.Fatal("shutdown returned while accepted processors were still blocked")
	default:
	}
	unblock()
	waitSignal(t, shutdownDone, "draining accepted tasks before shutdown returns")
	shutdownPool(t, pool)
	for i, task := range tasks[:4] {
		stored, _ := store.GetTask(task.ID)
		if stored.Status != Completed || stored.Result == nil || stored.Error != "" {
			t.Fatalf("accepted task was not drained: %+v", stored)
		}
		checkStatistics(t, *stored.Result, Statistics{
			Count: 2, Min: float64(i), Max: float64(i + 2),
			Sum: float64(2*i + 2), Average: float64(i + 1),
		})
	}
	activityMu.Lock()
	defer activityMu.Unlock()
	if peak != 2 || active != 0 || calls != 4 {
		t.Fatalf("expected two overlapping reusable workers and four calls; peak=%d active=%d calls=%d", peak, active, calls)
	}
}

func TestWorkerPoolRejectsUnknownAndTerminalTasks(t *testing.T) {
	store := NewTaskStore(nil)
	completed := createPoolTask(t, store, 1)
	failed := createPoolTask(t, store, math.MaxFloat64, math.MaxFloat64)
	if _, err := store.ProcessTask(completed.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ProcessTask(failed.ID); err == nil {
		t.Fatal("expected overflow failure")
	}
	pool, err := NewWorkerPool(store, 2, 2)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { shutdownPool(t, pool) })
	if err := pool.Submit(999); !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("unknown task: %v", err)
	}
	for _, id := range []int{completed.ID, failed.ID} {
		if err := pool.Submit(id); !errors.Is(err, ErrInvalidTransition) {
			t.Fatalf("terminal task %d: %v", id, err)
		}
	}
}

func TestWorkerPoolPreservesFailureDetails(t *testing.T) {
	store := NewTaskStore(brokenProcessor{})
	pool, err := NewWorkerPool(store, 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { shutdownPool(t, pool) })
	task := createPoolTask(t, store, 1)
	if err := pool.Submit(task.ID); err != nil {
		t.Fatal(err)
	}
	shutdownPool(t, pool)
	stored, _ := store.GetTask(task.ID)
	if stored.Status != Failed || stored.Result != nil || stored.Error == "" {
		t.Fatalf("expected observable failure without successful result: %+v", stored)
	}
}

func TestWorkerPoolConcurrentCreateSubmitAndSnapshots(t *testing.T) {
	const count = 80
	store := NewTaskStore(nil)
	pool, err := NewWorkerPool(store, 3, count)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { shutdownPool(t, pool) })
	start := make(chan struct{})
	ids := make(chan int, count)
	errorsSeen := make(chan error, count+1)
	var producers sync.WaitGroup
	for i := 1; i <= count; i++ {
		producers.Add(1)
		go func(value int) {
			defer producers.Done()
			<-start
			task, err := store.CreateTask([]float64{float64(value), float64(value + 2)})
			if err != nil {
				errorsSeen <- err
				return
			}
			ids <- task.ID
			task.Numbers[0] = -999 
			if err := pool.Submit(task.ID); err != nil {
				errorsSeen <- err
			}
		}(i)
	}
	stopReads := make(chan struct{})
	readsDone := make(chan struct{})
	go func() {
		defer close(readsDone)
		<-start
		for {
			for _, snapshot := range store.ListTasks() {
				if _, err := store.GetTask(snapshot.ID); err != nil {
					errorsSeen <- err
					return
				}
				snapshot.Numbers[0] = -555
				if snapshot.Result != nil {
					snapshot.Result.Sum = -555
				}
			}
			select {
			case <-stopReads:
				return
			default:
				runtime.Gosched()
			}
		}
	}()
	close(start)
	producersDone := make(chan struct{})
	go func() { producers.Wait(); close(producersDone) }()
	waitSignal(t, producersDone, "concurrent creators and submitters")
	shutdownPool(t, pool)
	close(stopReads)
	waitSignal(t, readsDone, "concurrent snapshot reader")
	close(errorsSeen)
	for err := range errorsSeen {
		t.Errorf("concurrent operation failed: %v", err)
	}
	close(ids)
	unique := make(map[int]bool)
	for id := range ids {
		if unique[id] {
			t.Fatalf("duplicate generated task ID %d", id)
		}
		unique[id] = true
	}
	if len(unique) != count || len(store.ListTasks()) != count {
		t.Fatalf("lost tasks: %d unique IDs, %d stored", len(unique), len(store.ListTasks()))
	}
	for _, task := range store.ListTasks() {
		if task.Status != Completed || task.Result == nil {
			t.Fatalf("task did not complete: %+v", task)
		}
		first := task.Numbers[0]
		if first < 1 || first > count {
			t.Fatalf("snapshot mutation escaped into storage: %+v", task)
		}
		checkStatistics(t, *task.Result, Statistics{Count: 2, Min: first, Max: first + 2, Sum: 2*first + 2, Average: first + 1})
	}
}

func TestWorkerPoolConcurrentDuplicateSubmission(t *testing.T) {
	const contenders = 40
	store := NewTaskStore(nil)
	pool, err := NewWorkerPool(store, 2, contenders)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { shutdownPool(t, pool) })
	task := createPoolTask(t, store, 2, 4)
	start := make(chan struct{})
	results := make(chan error, contenders)
	for i := 0; i < contenders; i++ {
		go func() { <-start; results <- pool.Submit(task.ID) }()
	}
	close(start)
	accepted := 0
	for i := 0; i < contenders; i++ {
		select {
		case err := <-results:
			if err == nil {
				accepted++
			} else if !errors.Is(err, ErrInvalidTransition) {
				t.Fatalf("unexpected duplicate submission error: %v", err)
			}
		case <-time.After(concurrentTestTimeout):
			t.Fatal("concurrent duplicate submission blocked")
		}
	}
	shutdownPool(t, pool)
	if accepted != 1 {
		t.Fatalf("same task accepted %d times", accepted)
	}
	stored, _ := store.GetTask(task.ID)
	if stored.Status != Completed || stored.Result == nil || stored.Result.Sum != 6 {
		t.Fatalf("wrong result after duplicate contenders: %+v", stored)
	}
}

func TestWorkerPoolSubmitRacingRepeatedShutdown(t *testing.T) {
	const count = 100
	store := NewTaskStore(nil)
	pool, err := NewWorkerPool(store, 2, count)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { shutdownPool(t, pool) })
	tasks := make([]Task, count)
	for i := range tasks {
		tasks[i] = createPoolTask(t, store, float64(i+1))
	}

	if err := pool.Submit(tasks[0].ID); err != nil {
		t.Fatal(err)
	}
	type outcome struct {
		id  int
		err error
	}
	outcomes := make(chan outcome, count-1)
	start := make(chan struct{})
	var group sync.WaitGroup
	for _, task := range tasks[1:] {
		group.Add(1)
		go func(id int) {
			defer group.Done()
			<-start
			outcomes <- outcome{id, pool.Submit(id)}
		}(task.ID)
	}
	for i := 0; i < 5; i++ {
		group.Add(1)
		go func() { defer group.Done(); <-start; pool.Shutdown() }()
	}
	close(start)
	done := make(chan struct{})
	go func() { group.Wait(); close(done) }()
	waitSignal(t, done, "submissions racing repeated shutdown")
	close(outcomes)
	accepted := map[int]bool{tasks[0].ID: true}
	for result := range outcomes {
		if result.err == nil {
			accepted[result.id] = true
		} else if !errors.Is(result.err, ErrPoolStopped) {
			t.Errorf("unexpected race outcome for task %d: %v", result.id, result.err)
		}
	}
	for _, created := range tasks {
		task, _ := store.GetTask(created.ID)
		if accepted[task.ID] {
			if task.Status != Completed || task.Result == nil || task.Result.Sum != created.Numbers[0] {
				t.Errorf("accepted task lost during shutdown: %+v", task)
			}
		} else if task.Status != Pending || task.Result != nil {
			t.Errorf("rejected task was processed: %+v", task)
		}
	}
	if err := pool.Submit(tasks[0].ID); !errors.Is(err, ErrPoolStopped) {
		t.Fatalf("submission after shutdown: %v", err)
	}
}
