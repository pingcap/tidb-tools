package tools

// This file ensures `go mod tidy` will not delete entries to all tools.

import (
	// failpoint enables manual 'failure' of some execution points.
	_ "github.com/pingcap/failpoint"
)
