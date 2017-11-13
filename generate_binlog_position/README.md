## gen_meta

gen_meta is a tool to generate fake drainer savepoint file.

## How to use

```
Usage of gen_meta:
  --pd-urls string
	a comma separated list of PD endpoints
  --data-dir string
	binlog position data directory path (default "binlog_position")
```

## Example
```
./bin/generate_binlog_position --pd-urls="http://127.0.0.1:2375" --data-dir="data.example"
>cat data.example/savePoint
commitTS = 395986424387338242
```
