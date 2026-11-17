package main

import (
	"fmt"
	"sort"
	"sync"
)

type TaskStore struct {
	mu        sync.RWMutex 
	tasks     map[int]*Task
	nextID    int
	processor TaskProcessor
}

func NewTaskStore(processor TaskProcessor) *TaskStore {
	if processor == nil {
		processor = StatisticsProcessor{}
	}
	return &TaskStore{
		tasks:     make(map[int]*Task),
		nextID:    1,
		processor: processor,
	}
}

func (store *TaskStore) CreateTask(numbers []float64) (Task, error) {
	if err := validateNumbers(numbers); err != nil {
		return Task{}, err
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	task := &Task{
		ID:      store.nextID,
		Numbers: append([]float64(nil), numbers...),
		Status:  Pending,
	}
	store.tasks[task.ID] = task
	store.nextID++
	return cloneTask(*task), nil
}

func (store *TaskStore) GetTask(id int) (Task, error) {
	store.mu.RLock()
	defer store.mu.RUnlock()
	task, exists := store.tasks[id]
	if !exists {
		return Task{}, fmt.Errorf("%w: ID %d", ErrTaskNotFound, id)
	}
	return cloneTask(*task), nil
}

func (store *TaskStore) ProcessTask(id int) (Task, error) {
	return store.processTask(id, Pending)
}

func (store *TaskStore) processTask(id int, expected Status) (Task, error) {
	store.mu.Lock()
	task, exists := store.tasks[id]
	if !exists {
		store.mu.Unlock()
		return Task{}, fmt.Errorf("%w: ID %d", ErrTaskNotFound, id)
	}
	if task.Status != expected {
		status := task.Status
		store.mu.Unlock()
		return Task{}, fmt.Errorf("%w: task %d is %s", ErrInvalidTransition, id, status)
	}

	task.Status = Processing
	numbers := append([]float64(nil), task.Numbers...)
	store.mu.Unlock()

	result, err := store.processor.Calculate(numbers)

	store.mu.Lock()
	defer store.mu.Unlock()
	if err != nil {
		task.Status = Failed
		task.Error = err.Error()
		return Task{}, fmt.Errorf("processing task %d: %w", id, err)
	}

	task.Result = &result
	task.Status = Completed
	return cloneTask(*task), nil
}

func (store *TaskStore) ListTasks() []Task {
	store.mu.RLock()
	defer store.mu.RUnlock()
	ids := make([]int, 0, len(store.tasks))
	for id := range store.tasks {
		ids = append(ids, id)
	}
	sort.Ints(ids)

	tasks := make([]Task, 0, len(ids))
	for _, id := range ids {
		tasks = append(tasks, cloneTask(*store.tasks[id]))
	}
	return tasks
}
