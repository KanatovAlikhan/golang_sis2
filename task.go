package main

import (
	"errors"
	"fmt"
	"math"
)

type Status string

const (
	Pending    Status = "Pending"
	Queued     Status = "Queued"
	Processing Status = "Processing"
	Completed  Status = "Completed"
	Failed     Status = "Failed"
)

var (
	ErrInvalidInput      = errors.New("invalid task input")
	ErrTaskNotFound      = errors.New("task not found")
	ErrInvalidTransition = errors.New("invalid task state transition")
)

type Statistics struct {
	Count   int
	Min     float64
	Max     float64
	Sum     float64
	Average float64
}

type Task struct {
	ID      int
	Numbers []float64
	Status  Status
	Result  *Statistics
	Error   string
}

func cloneTask(task Task) Task {
	task.Numbers = append([]float64(nil), task.Numbers...)
	if task.Result != nil {
		resultCopy := *task.Result
		task.Result = &resultCopy
	}
	return task
}

func validateNumbers(numbers []float64) error {
	if len(numbers) == 0 {
		return fmt.Errorf("%w: enter at least one number", ErrInvalidInput)
	}
	for _, number := range numbers {
		if math.IsNaN(number) || math.IsInf(number, 0) {
			return fmt.Errorf("%w: NaN and infinity are not allowed", ErrInvalidInput)
		}
	}
	return nil
}
