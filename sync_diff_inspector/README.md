# sync-diff-inspector

sync-diff-inspector is a tool for comparing two database's data.

## How to use

```
Usage of sync_diff_inspector:
  -L string
        log level: debug, info, warn, error, fatal (default "info")
  -V    print version of sync_diff_inspector
  -check-thread-count int
        how many goroutines are created to check data (default 1)
  -chunk-size int
        diff check chunk size (default 1000)
  -config string
        Config file
  -fix-sql-file string
        the name of the file which saves sqls used to fix different data (default "fix.sql")
  -sample int
        the percent of sampling check (default 100)
  -source-snapshot string
        source database's snapshot config
  -target-snapshot string
        target database's snapshot config
```

For more details you can read the [config.toml](./config.toml) and [config_sharding.toml](./config_sharding.toml).