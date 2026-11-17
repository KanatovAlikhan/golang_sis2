# SIS Checkpoint 2: Concurrent Statistics Calculator

This Go application extends the Checkpoint 1 statistics calculator with a worker pool. Each task contains a list of numbers. Processing calculates its count, minimum, maximum, sum, and arithmetic average, then stores the result in memory.

For example, `[10, 20, 30]` produces `Count=3, Min=10, Max=30, Sum=60, Average=20`. The original calculation, validation, unique IDs, task retrieval, and synchronous processing API are preserved. The Go module remains `sis-statistics-checkpoint1` to continue the same project.

## Run the application

The inherited `go.mod` declares **Go 1.27**. Use a compatible Go toolchain, open a terminal in this directory, and run:

```bash
go run .
```

The interactive menu offers:

```text
1. Create a task
2. Submit a task for processing
3. Get a task by ID
4. List all tasks
0. Exit
```

Create a task by entering numbers separated by spaces, then submit its ID using option 2. Submission returns after the task enters the queue; it does not wait for calculation. Use options 3 or 4 to inspect current states and results. These calculations are fast, so a task may already be `Completed` by the time you inspect it. The application uses no artificial delays.

Choosing 0 stops submissions, finishes accepted tasks, waits for the workers, and displays the final task states. End of input also waits for accepted work before the application returns. Memory is not persisted between runs.

## Worker pool and queue

The CLI starts **2 workers** and uses a buffered channel with **capacity 3**. `NewWorkerPool(store, workerCount, queueCapacity)` starts the workers immediately and rejects a nil store, fewer than two workers, or a queue capacity below one.

```text
CreateTask -> in-memory store (Pending)
                      |
                 Submit(task ID)
                      |
             buffered channel (3 IDs)
                  /           \
              Worker 1     Worker 2
                  \           /
               calculate independently
                      |
          update corresponding stored task
```

Workers repeatedly receive IDs from the same channel and process independent tasks concurrently. The pool reuses a fixed number of goroutines instead of creating one per task. Completion order is not guaranteed.

The queue capacity limits **waiting** tasks. Up to two additional tasks can be actively processing, and the store can also contain unsubmitted or finished tasks. If the queue is full, `Submit` immediately returns `ErrQueueFull`; the rejected task stays `Pending` and can be submitted again when space is available. No task is silently dropped.

## Task lifecycle and errors

```text
Pending -> Queued -> Processing -> Completed
                               \-> Failed
```

- `CreateTask(numbers)` validates and copies input, assigns a unique integer ID, and stores a `Pending` task.
- Only `Pending` tasks can be submitted. An accepted submission marks the task `Queued` atomically with queue insertion.
- A worker changes the task to `Processing` before calculating.
- Success stores its `Statistics` and marks it `Completed`.
- A calculation error marks it `Failed`, stores a message in `Task.Error`, and leaves `Result` nil. Retrieve the task to inspect asynchronous errors.
- `Queued`, `Processing`, `Completed`, and `Failed` tasks cannot be submitted again. Retries of finished tasks are not supported; create a new task instead.

The original `store.ProcessTask(id)` remains available for synchronous Checkpoint 1 callers and tests. It uses the same safe processing logic with the transition `Pending -> Processing -> Completed/Failed`. It rejects queued or already processed tasks. The CLI uses the worker pool for processing.

Input must contain at least one finite number. `NaN` and infinities are rejected before a task is stored; a sum that overflows during calculation fails processing. Errors use ordinary Go error returns and can be checked with `errors.Is`:

| Error | Meaning |
| --- | --- |
| `ErrInvalidInput` | Empty/nonfinite input, or an overflowing calculation. |
| `ErrTaskNotFound` | No task has the requested ID. |
| `ErrInvalidTransition` | The task cannot be submitted or processed in its current state. |
| `ErrQueueFull` | The pool is running, but its waiting queue has no room. |
| `ErrPoolStopped` | Shutdown has started; the pool accepts no new submissions. |
| `ErrInvalidPoolConfig` | The pool configuration is invalid. |

Creating and retrieving tasks remain available after pool shutdown, but those new tasks cannot be submitted to the stopped pool. A stopped pool cannot be restarted.

## Synchronization and shutdown

`TaskStore` uses a `sync.RWMutex` to protect its map, ID counter, task states, results, and errors. It copies input slices on creation and returns deep snapshots from create/get/list/process operations. Callers cannot mutate stored data through those returned values. Each calculation also receives its own input copy.

State validation and updates happen under the store lock, so two callers cannot reserve the same pending task. Calculation runs **outside** that lock, allowing other workers and readers to proceed. `StatisticsProcessor` is stateless; a custom `TaskProcessor` must support concurrent `Calculate` calls.

The pool has a separate mutex protecting submission and shutdown. Submission takes the pool lock before the store lock; workers do not take the pool lock. Shutdown marks the pool stopped and closes its channel while holding the pool lock, then releases it before waiting on a `sync.WaitGroup`. This prevents a concurrent sender from using a closed channel and avoids waiting while holding a lock needed by workers.

Only the pool closes the queue. Workers range over the channel, finish all accepted work after closure, then terminate. Every `Shutdown()` call waits for the workers; repeated and concurrent calls are safe. Normal shutdown drains the queue rather than interrupting calculations, so custom processors must eventually return. Context cancellation and forced interruption are outside this checkpoint.

## Project files

| File | Responsibility |
| --- | --- |
| `go.mod` | Original module and Go version. |
| `main.go` | Interactive CLI, number parsing, and pool shutdown. |
| `task.go` | Task/result types, statuses, validation, errors, and copying. |
| `processor.go` | Processor interface and statistics calculations. |
| `store.go` | Synchronized in-memory storage and task processing. |
| `worker_pool.go` | Fixed workers, bounded queue, submission, and shutdown. |
| `store_test.go` | Checkpoint 1 behavior and calculation tests. |
| `worker_pool_test.go` | Concurrency, queue, state, and shutdown tests. |
| `DEFENSE_GUIDE.md` | Short explanations and questions for code defense. |

All application code uses the Go standard library. No database, HTTP service, or external concurrency package is needed.

## Format, check, and test

Run from this project directory:

```bash
gofmt -l .
go vet ./...
go test ./...
go test -race ./...
```

`gofmt -l .` should print nothing. To format source files, run `gofmt -w *.go`. The race detector requires a supported Go platform and a C compiler with CGO enabled.

Tests verify actual overlap, the worker limit, correct results and states, invalid or duplicate submissions, a full queue, safe concurrent access, draining shutdown, and rejection after stopping. Channel coordination makes the concurrency checks independent of worker completion order; timeouts only detect hangs. Passing the race detector supplements these behavioral assertions.

## Submit and defend

Update the **same public GitHub repository used for Checkpoint 1**, preserving its history. Submit its link and the corresponding commit link or tag through Microsoft Teams before the announced deadline. Check that the repository is accessible without requesting access.

If you received this project as an archive, extract it and copy the source and documentation into your existing Checkpoint 1 checkout. Keep that checkout's `.git` directory. Run the checks above, then commit and push the changes to that same repository.

Prepare to explain your submitted code during the mandatory individual defense. Review `DEFENSE_GUIDE.md` and follow the practice-session schedule announced by the course.
