package main

import (
	"errors"
	"math"
	"testing"
)

func TestCreateTask(t *testing.T) {
	store := NewTaskStore(StatisticsProcessor{})
	input := []float64{10, -5, 4}

	task, err := store.CreateTask(input)
	if err != nil {
		t.Fatalf("CreateTask returned an unexpected error: %v", err)
	}
	if task.ID != 1 || task.Status != Pending || task.Result != nil {
		t.Fatalf("wrong new task: %+v", task)
	}

	input[0] = 100
	stored, err := store.GetTask(task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Numbers[0] != 10 {
		t.Fatalf("input was not copied: got %v", stored.Numbers)
	}
}

func TestInvalidInput(t *testing.T) {
	tests := []struct {
		name    string
		numbers []float64
	}{
		{"nil slice", nil},
		{"empty slice", []float64{}},
		{"not a number", []float64{math.NaN()}},
		{"positive infinity", []float64{1, math.Inf(1)}},
		{"negative infinity", []float64{math.Inf(-1)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := NewTaskStore(StatisticsProcessor{})
			_, err := store.CreateTask(tt.numbers)
			if !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("expected ErrInvalidInput, got %v", err)
			}
			if len(store.ListTasks()) != 0 {
				t.Fatal("invalid task should not be stored")
			}
		})
	}
}

func TestGetTaskNotFound(t *testing.T) {
	store := NewTaskStore(StatisticsProcessor{})
	_, err := store.GetTask(999)
	if !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("expected ErrTaskNotFound, got %v", err)
	}
	_, err = store.ProcessTask(999)
	if !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("expected ErrTaskNotFound from ProcessTask, got %v", err)
	}
}

func TestProcessTaskResults(t *testing.T) {
	tests := []struct {
		name     string
		input    []float64
		expected Statistics
	}{
		{"three positives", []float64{10, 20, 30}, Statistics{Count: 3, Min: 10, Max: 30, Sum: 60, Average: 20}},
		{"negative numbers", []float64{-5, -1, 2}, Statistics{Count: 3, Min: -5, Max: 2, Sum: -4, Average: -4.0 / 3}},
		{"single number", []float64{7}, Statistics{Count: 1, Min: 7, Max: 7, Sum: 7, Average: 7}},
		{"decimal numbers", []float64{0.1, 0.2, 0.3}, Statistics{Count: 3, Min: 0.1, Max: 0.3, Sum: 0.6, Average: 0.2}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := NewTaskStore(StatisticsProcessor{})
			created, err := store.CreateTask(tt.input)
			if err != nil {
				t.Fatal(err)
			}
			completed, err := store.ProcessTask(created.ID)
			if err != nil {
				t.Fatalf("ProcessTask failed: %v", err)
			}
			if completed.Status != Completed || completed.Result == nil {
				t.Fatalf("task should be completed with result: %+v", completed)
			}
			checkStatistics(t, *completed.Result, tt.expected)

			fromStore, err := store.GetTask(created.ID)
			if err != nil {
				t.Fatal(err)
			}
			if fromStore.Status != Completed || fromStore.Result == nil {
				t.Fatalf("completed task was not stored: %+v", fromStore)
			}
			checkStatistics(t, *fromStore.Result, tt.expected)
		})
	}
}

func checkStatistics(t *testing.T, got, want Statistics) {
	t.Helper()
	if got.Count != want.Count ||
		math.Abs(got.Min-want.Min) > 1e-9 ||
		math.Abs(got.Max-want.Max) > 1e-9 ||
		math.Abs(got.Sum-want.Sum) > 1e-9 ||
		math.Abs(got.Average-want.Average) > 1e-9 {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestProcessCompletedTaskIsRejected(t *testing.T) {
	store := NewTaskStore(StatisticsProcessor{})
	task, _ := store.CreateTask([]float64{10, 20})
	_, err := store.ProcessTask(task.ID)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.ProcessTask(task.ID)
	if !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("expected ErrInvalidTransition, got %v", err)
	}
	stored, _ := store.GetTask(task.ID)
	if stored.Status != Completed {
		t.Fatalf("status should stay Completed, got %s", stored.Status)
	}
}

func TestMultipleTasksRemainIndependent(t *testing.T) {
	store := NewTaskStore(StatisticsProcessor{})
	first, _ := store.CreateTask([]float64{1, 3})
	second, _ := store.CreateTask([]float64{100, 200})
	if first.ID == second.ID {
		t.Fatal("two tasks have the same ID")
	}

	_, err := store.ProcessTask(second.ID)
	if err != nil {
		t.Fatal(err)
	}
	stillPending, _ := store.GetTask(first.ID)
	if stillPending.Status != Pending || stillPending.Result != nil {
		t.Fatalf("the first task changed unexpectedly: %+v", stillPending)
	}

	_, err = store.ProcessTask(first.ID)
	if err != nil {
		t.Fatal(err)
	}
	one, _ := store.GetTask(first.ID)
	two, _ := store.GetTask(second.ID)
	if one.Result.Average != 2 || two.Result.Average != 150 {
		t.Fatalf("task results were mixed up: %v / %v", one.Result, two.Result)
	}
}

type brokenProcessor struct{}

func (brokenProcessor) Calculate(_ []float64) (Statistics, error) {
	return Statistics{}, errors.New("cannot calculate")
}

func TestProcessingErrorChangesStatusToFailed(t *testing.T) {
	store := NewTaskStore(brokenProcessor{})
	task, _ := store.CreateTask([]float64{1})
	_, err := store.ProcessTask(task.ID)
	if err == nil {
		t.Fatal("expected a processing error")
	}
	stored, _ := store.GetTask(task.ID)
	if stored.Status != Failed || stored.Result != nil {
		t.Fatalf("processing error should set Failed without result, got %+v", stored)
	}
	_, err = store.ProcessTask(task.ID)
	if !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("failed tasks must not be reprocessed, got %v", err)
	}
}

func TestOverflowChangesStatusToFailed(t *testing.T) {
	store := NewTaskStore(StatisticsProcessor{})
	task, err := store.CreateTask([]float64{math.MaxFloat64, math.MaxFloat64})
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.ProcessTask(task.ID)
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected overflow error, got %v", err)
	}
	stored, _ := store.GetTask(task.ID)
	if stored.Status != Failed {
		t.Fatalf("expected Failed, got %s", stored.Status)
	}
}

func TestGetTaskReturnsSafeCopy(t *testing.T) {
	store := NewTaskStore(StatisticsProcessor{})
	task, _ := store.CreateTask([]float64{4, 8})
	processed, err := store.ProcessTask(task.ID)
	if err != nil {
		t.Fatal(err)
	}
	processed.Numbers[0] = 999
	processed.Result.Average = 999

	stored, _ := store.GetTask(task.ID)
	if stored.Numbers[0] != 4 || stored.Result.Average != 6 {
		t.Fatalf("caller modified internal store data: %+v", stored)
	}
}

func TestListTasksSorted(t *testing.T) {
	store := NewTaskStore(StatisticsProcessor{})
	for _, n := range []float64{1, 2, 3} {
		_, err := store.CreateTask([]float64{n})
		if err != nil {
			t.Fatal(err)
		}
	}
	all := store.ListTasks()
	if len(all) != 3 || all[0].ID != 1 || all[1].ID != 2 || all[2].ID != 3 {
		t.Fatalf("wrong list order: %+v", all)
	}
	all[0].Numbers[0] = 500
	stored, _ := store.GetTask(1)
	if stored.Numbers[0] != 1 {
		t.Fatal("ListTasks must return copies")
	}
}

func TestParseNumbers(t *testing.T) {
	numbers, err := parseNumbers("1  -2.5\t3")
	if err != nil || len(numbers) != 3 || numbers[1] != -2.5 {
		t.Fatalf("got %v and error %v", numbers, err)
	}
	_, err = parseNumbers("1 hello")
	if err == nil {
		t.Fatal("non-numeric input must be rejected")
	}
}
