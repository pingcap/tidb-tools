package precheck

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
