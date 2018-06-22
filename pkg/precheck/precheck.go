package precheck

import (
	"sync"

	"github.com/juju/errors"
	"github.com/ngaut/log"
)

// PreChecker is interface about precheck.
type PreChecker interface {
	Name() string
	PreCheck() *PreCheckResult
}

// PreCheckState is state of precheck
type PreCheckState string

const (
	PreCheckState_Success PreCheckState = "success"
	PreCheckState_Failure PreCheckState = "fail"
	PreCheckState_Warning PreCheckState = "warn"
)

// PreCheckResult is result of precheck
type PreCheckResult struct {
	Id          uint64        `json:"id"`
	Name        string        `json:"name"`
	Desc        string        `json:"desc"`
	State       PreCheckState `json:"state"`
	ErrorMsg    string        `json:"errorMsg"`
	Instruction string        `json:"instruction"`
	Extra       string        `json:"extra"`
}

// PreCheckResultSummary is summary of all precheck results
type PreCheckResultSummary struct {
	Passed     bool  `json:"passed"`
	Total      int64 `json:"total"`
	Successful int64 `json:"successful"`
	Failed     int64 `json:"failed"`
	Warning    int64 `son:"warning"`
}

// PreCheckResults contains all precheck results and summary
type PreCheckResults struct {
	Results []*PreCheckResult      `json:"results"`
	Summary *PreCheckResultSummary `json:"summary"`
}

// DoPreCheck executes several precheckers.
func DoPreCheck(precheckers []PreChecker) (*PreCheckResults, error) {
	results := &PreCheckResults{
		Results: make([]*PreCheckResult, 0, len(precheckers)),
	}
	if len(precheckers) == 0 {
		results.Summary = &PreCheckResultSummary{Passed: true}
		return results, nil
	}

	var (
		wg         sync.WaitGroup
		finished   bool
		total      int64
		successful int64
		failed     int64
		warning    int64
		err        error
	)
	total = int64(len(precheckers))

	resultCh := make(chan *PreCheckResult)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case result := <-resultCh:
				switch result.State {
				case PreCheckState_Success:
					successful++
				case PreCheckState_Failure:
					failed++
				case PreCheckState_Warning:
					warning++
				}

				// if total == successful + warning + failed, it's finished
				finished = (total == successful+warning+failed)
				results.Results = append(results.Results, result)

				log.Debugf("[precheck] precheck request %s result:%+v", result, result)
				if finished {
					return
				}
			}
		}
	}()

	for i, prechecker := range precheckers {
		wg.Add(1)
		go func(i int, prechecker PreChecker) {
			defer wg.Done()
			result := prechecker.PreCheck()
			result.Id = uint64(i)
			resultCh <- result
		}(i, prechecker)
	}
	wg.Wait()

	passed := finished && (failed == 0)
	results.Summary = &PreCheckResultSummary{
		Passed:     passed,
		Total:      total,
		Successful: successful,
		Failed:     failed,
		Warning:    warning,
	}

	log.Infof("[precheck] precheck finished, passed %v / total %v", passed, total)
	return results, errors.Trace(err)
}
