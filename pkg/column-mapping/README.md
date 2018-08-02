# Column Mapping

## introduction

column mapping is a library to provide a simple and unified way to mapping columns of table:

- add prefix for one column

- add suffix for one column
    
- clone one column

## column mapping rule

we define a rule `Rule` to show how to map column

```go
type Rule struct {
	PatternSchema    string   `yaml:"pattern-schema" json:"parttern-schema" toml:"pattern-schema"`
	PatternTable     string   `yaml:"pattern-table" json:"pattern-table" toml:"pattern-table"`
	SourceColumn     string   `yaml:"source-column" json:"source-column" toml:"source-column"` // modify, add refer column, ignore
	TargetColumn     string   `yaml:"target-column" json:"target-column" toml:"target-column"` // add column, modify
	Expression       string   `yaml:"expression" json:"expression" toml:"expression"`
	Arguments        []string `yaml:"arguments" json:"arguments" toml:"arguments"`
	CreateTableQuery string   `yaml:"create-table-query" json:"create-table-query" toml:"create-table-query"`
}
```

now we support following expressions

``` go
addPrefix
addSuffix
clone
```

## notice
* only support some poor expressions, we would unify tableInfo later and support more
* don't support modify DDL automatically, we would implement it later 