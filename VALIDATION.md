# Checkpoint 2 validation

Validated on Linux amd64 using Go 1.27.4, with CGO enabled for the race detector. The supplied repository was cloned at Checkpoint 1 commit `a5659efddcf5a505c8cb54cfcbbefbc494980ca1`; its files matched the uploaded Checkpoint 1 project before these changes were applied.

| Check | Result |
| --- | --- |
| Original Checkpoint 1 unit tests before changes | Passed |
| Original tests retained in the upgraded project | Passed; only gofmt changes to the test file |
| `gofmt -l .` | Passed; no output |
| `go vet ./...` | Passed |
| `go test ./...` | Passed |
| `go test -race ./...` | Passed; no races reported |
| `go build` | Passed in the actual repository checkout |
| CLI: two accepted tasks followed by normal exit | Both completed with correct independent results |
| CLI: end of input after submission | Accepted task completed before exit |
| CLI: arithmetic overflow during asynchronous processing | Failed status and error message displayed |
| CLI: invalid input and unknown task ID | Errors displayed without a panic |

Concurrency tests coordinate workers using channels to demonstrate overlapping calculations, a fixed worker limit, exact queue saturation, duplicate prevention, copied snapshots, and draining shutdown while submissions race with repeated shutdown calls. Timeouts detect hangs rather than simulate work.

These checks validate the supplied source. Run the README commands again in the repository version you submit.
