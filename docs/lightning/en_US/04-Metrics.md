Metrics
=======

Both `tidb-lightning` and `tikv-importer` supports metrics collection via
[Prometheus](https://prometheus.io/).

Configuration
-------------

If you installed Lightning via TiDB-Ansible, simply adding the servers to the `[monitored_servers]`
section in the `inventory.ini` should be sufficient to let the Prometheus server to collect their
metrics.

If you installed Lightning manually, follow the instructions below.

### `tikv-importer`

`tikv-importer` v2.1 uses [Pushgateway](https://github.com/prometheus/pushgateway) to deliver
metrics. Configure `tikv-importer.toml` to recognize the Pushgateway with the following settings:

```toml
[metric]

# The Prometheus client push job name.
job = "tikv-importer"

# The Prometheus client push interval.
interval = "15s"

# The Prometheus Pushgateway address.
address = ""
```

### `tidb-lightning`

The metrics of `tidb-lightning` can be gathered directly by Prometheus as long as it is discovered.
The metrics port can be set in `tidb-lightning.toml`

```toml
[lightning]
# HTTP port for debugging and Prometheus metrics pulling (0 to disable)
pprof-port = 10089

...
```

Prometheus needs to be configured to discover the `tidb-lightning` server. For instance, you could
hard-code the server address to the `scrape_configs` section:

```yaml
...
scrape_configs:
  - job_name: 'tidb-lightning'
    static_configs:
      - targets: ['192.168.20.10:10089']
```

Raw metrics
-----------

### `tikv-importer`

Metrics provided by `tikv-importer` are listed under the namespace `tikv_import_*`.

* **`tikv_import_rpc_duration`** (Histogram)

    Bucketed histogram of import RPC duration. Labels:

    * **request**: RPC name, e.g. `open_engine`, `import_engine`, etc.
    * **result**: `ok` / `error`

* **`tikv_import_write_chunk_bytes`** (Histogram)

    Bucketed histogram of import write chunk bytes.

* **`tikv_import_write_chunk_duration`** (Histogram)

    Bucketed histogram of import write chunk duration.

* **`tikv_import_upload_chunk_bytes`** (Histogram)

    Bucketed histogram of import upload chunk bytes.

* **`tikv_import_upload_chunk_duration`** (Histogram)

    Bucketed histogram of import upload chunk duration.

### `tidb-lightning`

Metrics provided by `tidb-lightning` are listed under the namespace `lightning_*`.

* **`lightning_importer_engine`** (Counter)

    Counting open and closed engine files. Labels:

    * **type**: `open` / `closed`

* **`lightning_idle_workers`** (Gauge)

    Counting idle workers. Values should be less than the `table-concurrency`/`region-concurrency`
    settings and are typically be zero. Labels:

    * **name**: `table` / `region`

* **`lightning_kv_encoder`** (Counter)

    Counting open and closed KV encoders. KV encoders are in-memory TiDB instances which converts
    SQL INSERT statements into KV pairs. The net values should be bounded in a healthy situation.
    Labels:

    * **type**: `open` / `closed`

* **`lightning_tables`** (Counter)

    Counting number of tables processed and their status. Labels:

    * **state**: `pending` / `written` / `closed` / `imported` / `altered_auto_inc` / `checksum` / `completed`
    * **result**: `success` / `failure`

* **`lightning_chunks`** (Counter)

    Counting number of chunks processed and their status. Labels:

    * **state**: `estimated` / `pending` / `running` / `finished` / `failed`
