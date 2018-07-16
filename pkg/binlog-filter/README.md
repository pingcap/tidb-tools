# Binlog Filter

## introduction

Binlog Filter is a libary to provide a simple and unified way to filter binlog events with the following features:

- Do/Ignore databases

    /Ignore replicated data from these databases.

- Do/Ignore tables
    
    Synchronize/Ignore replicated data from these tables.

- Do/Ignore binlog events
    
    Synchronize/Ignore some specified replicated `Binlog Events` from these specfied databases/tables by given rules.

- Do/Ignore binlog queries

    Synchronize/Ignore some specified replicated queries that is in `Binog Query Event` from these specfied databases/tables by given rules.

## binlog event rule

we define a rule `BinlogEventRule` to filter specified `Binlog Events` and queries that is in `Binog Query Event`

```go
type BinlogEventRule struct {
	SchemaPattern string      `jsSynchronizeon:"schema-pattern" toml:"schema-pattern"`
	TablePattern  string      `json:"table-pattern" toml:"table-pattern"`
	DMLEvent      []EventType `json:"dml" toml:"dml"`                 
	DDLEvent      []EventType `json:"ddl" toml:"ddl"`
	SQLPattern    []string    `json:"sql-pattern" toml:"sql-pattern"` // regular expression
	sqlRegularExp *regexp.Regexp

	Action ActionType `json:"action" toml:"action"`
}
```

now we support following events 

``` go
// it indicates all dml/ddl events in rule
AllEvent
    
// it indicates no any dml/ddl events in rule,
// and equals empty rule.DDLEvent/DMLEvent array
NoneEvent

// DML events
InsertEvent
UpdateEvent
DeleteEvent

// DDL events
CreateDatabase
DropDatabase
CreateTable
DropTable
TruncateTable
RenameTable
CreateIndex
DropIndex
AlertTable

// unknow event
NullEvent EventType = ""
```

## notice
if you want to use `BinlogEventRule` to synchronize/ignore some table, you may need to pay attention to setting `AllEvent` and `NoneEvent`.

like synchronizing all events from specified table or setting a do table, ignore is opposite.
``` go
BinlogEventRule {
	SchemaPattern: test*,
	TablePattern:  test*,
	DMLEvent:     []EventType{AllEvent},               
	DDLEvent:     []EventType{AllEvent},

    Action: Do,
}
```