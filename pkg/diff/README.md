# diff
## introduction
diff is a library to provide a function to compare tables.
To comapre tables, you should construct a TableDiff struct:
```go
// TableDiff saves config for diff table
type TableDiff struct {
	// source tables
	SourceTables []*TableInstance
	// target table
	TargetTable *TableInstance

	// columns be ignored
	IgnoreColumns []string

	// columns be removed
	RemoveColumns []string

	// field should be the primary key, unique key or field with index
	Field string

	// select range, for example: "age > 10 AND age < 20"
	Range string

	// for example, the whole data is [1...100]
	// we can split these data to [1...10], [11...20], ..., [91...100]
	// the [1...10] is a chunk, and it's chunk size is 10
	// size of the split chunk
	ChunkSize int

	// sampling check percent, for example 10 means only check 10% data
	Sample int

	// how many goroutines are created to check data
	CheckThreadCount int

	// set true if target-db and source-db all support tidb implicit column "_tidb_rowid"
	UseRowID bool

	// set false if want to comapre the data directly
    UseChecksum bool
    
    // collation config in mysql/tidb, should corresponding to charset.
	Collation string

	// ignore check table's struct
	IgnoreStructCheck bool

	// ignore check table's data
	IgnoreDataCheck bool
}
```


