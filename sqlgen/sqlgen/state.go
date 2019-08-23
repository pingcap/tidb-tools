package sqlgen

import "log"

type Choice struct {
	Branch int
	SeqNum int
}
type ResultType int

const (
	PlainString ResultType = iota
	NonExist
	Invalid
)

type Result struct {
	Tp    ResultType
	Value string
}

type State struct {
	Choices           []Choice
	Counter           map[string]int
	TotalCounter      map[string]int
	CurrentProduction *Production

	// Unchanged part during generation
	ProductionMap       map[string]*Production
	BeginProductionName string
	IsInitialize        bool
}

func (s *State) Parent() *Production {
	name := s.BeginProductionName
	path := s.Choices[:len(s.Choices)-1]
	for _, is := range path {
		prod, ok := s.ProductionMap[name]
		if !ok {
			return nil
		}
		name = prod.bodyList[is.Branch].seq[is.SeqNum]
	}
	prod, ok := s.ProductionMap[name]
	if !ok {
		return nil
	}
	return prod
}

func (s *State) Previous() Result {
	parent := s.Parent()
	if parent == nil {
		return Result{Tp: Invalid}
	}
	lastChoice := s.Choices[len(s.Choices)-1]
	if lastChoice.SeqNum == 0 {
		return Result{Tp: NonExist}
	}
	value := parent.bodyList[lastChoice.Branch].seq[lastChoice.SeqNum-1]
	return Result{Tp: PlainString, Value: value}
}

func (s *State) Next() Result {
	parent := s.Parent()
	if parent == nil {
		return Result{Tp: Invalid}
	}
	lastChoice := s.Choices[len(s.Choices)-1]
	seqs := parent.bodyList[lastChoice.Branch].seq
	if lastChoice.SeqNum >= len(seqs) {
		return Result{Tp: NonExist}
	}
	value := seqs[lastChoice.SeqNum+1]
	return Result{Tp: PlainString, Value: value}
}

func (s *State) Count() int {
	if s.CurrentProduction == nil {
		return 0
	}
	return s.Counter[s.CurrentProduction.head]
}

func (s *State) TotalCount() int {
	if s.CurrentProduction == nil {
		return 0
	}
	return s.TotalCounter[s.CurrentProduction.head]
}

func (s *State) EnsureInitialized() *State {
	if !s.IsInitialize {
		log.Fatal("The state is not initialized")
	}
	return s
}
