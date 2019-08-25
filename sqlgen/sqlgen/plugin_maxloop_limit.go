package sqlgen

// MaxLoopCounter implements sqlgen.ProductionListener.
type MaxLoopCounter struct {
	counter     map[string]int
	maxLoopback int
}

func NewMaxLoopCounter(maxLoopback int) MaxLoopCounter {
	return MaxLoopCounter{
		counter:     map[string]int{},
		maxLoopback: maxLoopback,
	}
}

func (pl *MaxLoopCounter) BeforeProductionGen(fn *Fn) {
	fnName := fn.Name
	pl.counter[fnName]++
	if pl.counter[fnName] > pl.maxLoopback {
		fn.F = InvalidF()
	}
}

func (pl *MaxLoopCounter) AfterProductionGen(fn *Fn, result *Result) {
	pl.counter[fn.Name]--
}

func (pl *MaxLoopCounter) ProductionCancel(fn *Fn) {
	pl.counter[fn.Name]--
}
