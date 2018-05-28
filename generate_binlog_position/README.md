## Generate_binlog_position

generate_binlog_position is a tool to generate fake drainer savepoint file.

## How to use

```
Usage of generate_binlog_position:
  --pd-urls string
	a comma separated list of PD endpoints
  --data-dir string
	binlog position data directory path (default "binlog_position")
```

## Example
```
./bin/generate_binlog_position --pd-urls="http://127.0.0.1:2379" --data-dir="data.example"
>cat data.example/savePoint
commitTS = 395986424387338242
```
