package sqlgen

// MaxLoopCounter implements sqlgen.ProductionListener.
type MaxLoopCounter struct {
	Counter     map[string]int
	MaxLoopback int
}

// NewMaxLoopCounter is a constructor for MaxLoopCounter, specifying maxLoopback.
func NewMaxLoopCounter(maxLoopback int) *MaxLoopCounter {
	return &MaxLoopCounter{
		Counter:     map[string]int{},
		MaxLoopback: maxLoopback,
	}
}

// BeforeProductionGen implements BeforeProductionGen for sqlgen.ProductionListener.
func (pl *MaxLoopCounter) BeforeProductionGen(fn *Fn) {
	fnName := fn.Name
	pl.Counter[fnName]++
	if pl.Counter[fnName] > pl.MaxLoopback {
		fn.F = InvalidF()
	}
}

// AfterProductionGen implements AfterProductionGen for sqlgen.ProductionListener.
func (pl *MaxLoopCounter) AfterProductionGen(fn *Fn, result *Result) {
	pl.Counter[fn.Name]--
}

// ProductionCancel implements ProductionCancel for sqlgen.ProductionListener.
func (pl *MaxLoopCounter) ProductionCancel(fn *Fn) {
	pl.Counter[fn.Name]--
}
