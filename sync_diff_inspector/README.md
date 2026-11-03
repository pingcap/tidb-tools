# sync-diff-inspector

sync-diff-inspector is a tool for comparing two database's data.

## How to use

```shell
Usage of diff:
  -V, --version                  print version of sync_diff_inspector
  -L, --log-level string         log level: debug, info, warn, error, fatal (default "info")
  -C, --config string            Config file
  -T, --template string          <dm|norm> export a template config file
      --dm-addr string           the address of DM
      --dm-task string           identifier of dm task
      --dm-allow-insecure-http    allow remote plaintext HTTP to DM (default false)
      --check-thread-count int   how many goroutines are created to check data (default 4)
      --export-fix-sql           set true if want to compare rows or set to false will only compare checksum (default true)
```

For more details you can read the [config.toml](./config/config.toml), [config_sharding.toml](./config/config_sharding.toml) and [config_dm.toml](./config/config_dm.toml).

## Resuming comparisons

Checkpoint reports retain the results of completed tables. A checkpoint missing
those results is rejected because it cannot safely reconstruct the verdict;
restart the comparison with a new output directory in that case.

## Connecting to DM

Remote `--dm-addr` endpoints require HTTPS. Local loopback HTTP remains supported.
For a private CA or mutual TLS, configure `[dm-security]` with `ca-path`,
`cert-path`, and `key-path` in the configuration file. The default HTTPS path
verifies the server against system roots. Redirects are rejected.

`--dm-allow-insecure-http` explicitly permits remote plaintext HTTP for trusted
networks. This sends database credentials without transport encryption.

DM passwords encrypted with the pre-v8.0 fixed key are a legacy compatibility
format, not a substitute for protecting configuration files and the DM API.
DM v8.0 and later support a deployment-specific `secret-key-path`; changing the
legacy decoder in this tool alone cannot protect existing DM configurations.

## Documents
- `zh`: [Overview in Chinese](https://docs.pingcap.com/zh/tidb/stable/sync-diff-inspector-overview)
- `en`: [Overview in English](https://docs.pingcap.com/tidb/stable/sync-diff-inspector-overview)
