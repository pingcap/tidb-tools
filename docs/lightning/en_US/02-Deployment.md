Deployment and Execution
========================

Hardware requirements
---------------------

### Notes

Before starting TiDB Lightning, note that:

- During the import process, the cluster cannot provide normal services.
- If `tidb-lightning` crashes, the cluster will be left in "import mode".
    Forgetting to switch back to "normal mode" will lead to a high amount of uncompacted data on
    the TiKV cluster, and will cause abnormally high CPU usage and stall.
    You can manually switch the cluster back to "normal mode" via the `tidb-lightning-ctl` tool:

    ```sh
    bin/tidb-lightning-ctl -switch-mode=normal
    ```

### Deploying to separate machines

`tidb-lightning` and `tikv-importer` are resource-intensive programs. It is recommended to deploy them into
two dedicated machines.

To achieve the best performance, it is recommended to use the following hardware configuration:

- `tidb-lightning`

    - 32+ logical cores CPU
    - 16 GB+ memory
    - 1 TB+ SSD, preferring higher read speed
    - 10 Gigabit network card
    - `tidb-lightning` fully consumes all CPU cores when running,
        and deploying on a dedicated machine is highly recommended.
        If not possible, `tidb-lightning` could be deployed together with other components like
        `tidb-server`, and limiting the CPU usage via the `region-concurrency` setting.

- `tikv-importer`

    - 32+ logical cores CPU
    - 32 GB+ memory
    - 1 TB+ SSD, preferring higher IOPS
    - 10 Gigabit network card
    - `tikv-importer` fully consumes all CPU, disk I/O and network bandwidth when running,
        and deploying on a dedicated machine is strongly recommended.
        If not possible, `tikv-importer` could be deployed together with other components like
        `tikv-server`, but the import speed might be affected.

If you have got enough machines, you could deploy multiple Lightning/Importer servers,
with each working on a distinct set of tables, to import the data in parallel.

### Deploying to single machine

If the hardware resources is severely under constraint, it is possible to deploy `tidb-lightning`
and `tikv-importer` and other components on the same machine, but note that the import performance
would also be impacted.

We recommend the following configuration of the single machine:

- 32+ logical cores CPU
- 32 GB+ memory
- 1 TB+ SSD, preferring higher IOPS
- 10 Gigabit network card

`tidb-lightning` is a CPU intensive program. In an environment with mixed components, the resources
allocated to `tidb-lightning` must be limited. Otherwise, other components might not be able to run.
We recommend setting the `region-concurrency` to 75% of CPU logical cores. For instance, if the CPU
has 32 logical cores, the `region-concurrency` can be set to 24.

Ansible deployment
------------------

TiDB Lightning can be deployed using Ansible, like [TiDB cluster itself][tidb-ansible].

[tidb-ansible]: https://github.com/pingcap/docs/blob/master/op-guide/ansible-deployment.md

1. Edit `inventory.ini` to provide the addresses of the `tidb-lightning` and `tikv-importer`
    servers:

    ```ini
    ...

    [importer_server]
    192.168.20.9

    [lightning_server]
    192.168.20.10

    ...
    ```

2. Configure these tools by editing the settings under `group_vars/*.yml`.

    * `group_vars/all.yml`

        ```yaml
        ...
        # The listening port of tikv-importer. Should be open to the tidb-lightning server.
        tikv_importer_port: 20170
        ...
        ```

    * `group_vars/lightning_server.yml`

        ```yaml
        ---
        dummy:

        # The listening port for metrics gathering. Should be open to the monitoring servers.
        tidb_lightning_pprof_port: 10089

        # The file path tidb-lightning reads the mydumper SQL dump from.
        data_source_dir: "{{ deploy_dir }}/mydumper"
        ```

    * `group_vars/importer_server.yml`

        ```yaml
        ---
        dummy:

        # The file path to store engine files. Should reside on a partition with large capacity.
        import_dir: "{{ deploy_dir }}/data.import"
        ```

3. Deploy the cluster via the usual steps

    ```sh
    ansible-playbook bootstrap.yml
    ansible-playbook deploy.yml
    ```

4. Mount the data source to the path specified in the `data_source_dir` setting.

5. Login to the `tikv-importer` server, and manually run

    ```sh
    scripts/start_importer.sh
    ```

    to start Importer.

6. Login to the `tidb-lightning` server, and manually run the following command
    to start Lightning and import the data into the TiDB cluster.

    ```sh
    scripts/start_lightning.sh
    ```

7. After completion, run `scripts/stop_importer.sh` on the `tikv-importer` server to stop Importer.

Manual deployment
-----------------

### TiDB cluster

Before importing, you should have deployed the TiDB cluster, with cluster version 2.0.4 or above.
Using the latest version is highly recommended.

You can find deployment instructions in the
[TiDB Quick Start Guide](https://pingcap.com/docs/QUICKSTART/).

Download the TiDB Lightning tool set (choose the one same as the cluster version):

- **v2.1**: https://download.pingcap.org/tidb-lightning-release-2.1-linux-amd64.tar.gz
- **v2.0**: https://download.pingcap.org/tidb-lightning-release-2.0-linux-amd64.tar.gz

### Starting `tikv-importer`

1. Upload `bin/tikv-importer` from the tool set.

2. Configure `tikv-importer.toml`:

    ```toml
    # TiKV Importer configuration file template

    # Log file
    log-file = "tikv-importer.log"
    # Log level: trace, debug, info, warn, error, off.
    log-level = "info"

    [server]
    # Listening address of tikv-importer. tidb-lightning needs to connect to
    # this address to write data.
    addr = "0.0.0.0:20170"
    # Size of thread pool for the gRPC server.
    grpc-concurrency = 16

    [metric]
    # The Prometheus client push job name.
    job = "tikv-importer"
    # The Prometheus client push interval.
    interval = "15s"
    # The Prometheus Pushgateway address.
    address = ""

    [rocksdb]
    # The maximum number of concurrent background jobs.
    max-background-jobs = 32

    [rocksdb.defaultcf]
    # Amount of data to build up in memory before flushing data to the disk.
    write-buffer-size = "1GB"
    # The maximum number of write buffers that are built up in memory.
    max-write-buffer-number = 8

    # The compression algorithms used in different levels.
    # The algorithm at level-0 is used to compress KV data.
    # The algorithm at level-6 is used to compress SST files.
    # The algorithms at level-1 to level-5 are unused for now.
    compression-per-level = ["lz4", "no", "no", "no", "no", "no", "zstd"]

    [import]
    # The directory to store engine files.
    import-dir = "/tmp/tikv/import"
    # Number of threads to handle RPC requests.
    num-threads = 16
    # Number of concurrent import jobs.
    num-import-jobs = 24
    # Maximum duration to prepare regions.
    #max-prepare-duration = "5m"
    # Split regions into this size according to the importing data.
    #region-split-size = "96MB"
    # Stream channel window size, stream will be blocked on channel full.
    #stream-channel-window = 128
    # Maximum number of open engines.
    max-open-engines = 8
    ```

3. Run `tikv-importer`.

    ```sh
    nohup ./tikv-importer -C tikv-importer.toml > nohup.out &
    ```

### Starting `tidb-lightning`

1. Upload `bin/tidb-lightning` and `bin/tidb-lightning-ctl` from the tool set.

2. Mount the mydumper SQL dump onto the same machine.

3. Configure `tidb-lightning.toml`:

    ```toml
    ### tidb-lightning configuartion

    [lightning]
    # HTTP port for debugging and Prometheus metrics pulling (0 to disable)
    pprof-port = 10089

    # check if the cluster satisfies the minimum requirement before starting
    #check-requirements = true

    # The maximum number of tables to be handled concurrently.
    # Must not exceed the max-open-engines setting for tikv-importer.
    table-concurrency = 8
    # The concurrency number of data. It is set to the number of logical CPU
    # cores by default. When deploying together with other components, you can
    # set it to 75% of the size of logical CPU cores to limit the CPU usage.
    #region-concurrency =

    # Logging
    level = "info"
    file = "tidb-lightning.log"
    max-size = 128 # MB
    max-days = 28
    max-backups = 14

    [checkpoint]
    # Whether to enable checkpoints.
    # While importing, Lightning records which tables have been imported, so
    # even if Lightning or other component crashed, we could start from a known
    # good state instead of redoing everything.
    enable = true
    # The schema name (database name) to store the checkpoints
    schema = "tidb_lightning_checkpoint"
    # The data source name (DSN) in the form "USER:PASS@tcp(HOST:PORT)/".
    # If not specified, the TiDB server from the [tidb] section will be used to
    # store the checkpoints. You could also specify a different MySQL-compatible
    # database server to reduce the load of the target TiDB cluster.
    #dsn = "root@tcp(127.0.0.1:4000)/"
    # Whether to keep the checkpoints after all data are imported. If false, the
    # checkpoints will be deleted. Keeping the checkpoints can aid debugging but
    # will leak metadata about the data source.
    #keep-after-success = false

    [tikv-importer]
    # The listening address of tikv-importer. Change it to the actual address
    addr = "172.16.31.10:20170"

    [mydumper]
    # Block size for file reading. Should be longer than the longest string of
    # the data source.
    read-block-size = 4096 # Byte (default = 4 KB)
    # Each data file will be split into multiple chunks of this size. Each chunk
    # will be processed in parallel.
    region-min-size = 268435456 # Byte (default = 256 MB)
    # mydumper local source data directory
    data-source-dir = "/data/my_database"
    # if no-schema is set true, tidb-lightning will assume the table skeletons
    # already exists on the target TiDB cluster, and will not execute the CREATE
    # TABLE statements
    no-schema = false

    [tidb]
    # Configuration of any one TiDB server from the cluster
    host = "172.16.31.1"
    port = 4000
    user = "root"
    password = ""
    # Table schema information is fetched from TiDB via this status-port.
    status-port = 10080
    # Address of any one PD server from the cluster
    pd-addr = "172.16.31.4:2379"
    # tidb-lightning imports TiDB as a library and generates some logs itself.
    # This setting controls the log level of the TiDB library.
    log-level = "error"
    # Sets TiDB session variable to speed up the Checksum and Analyze operations.
    distsql-scan-concurrency = 16

    # When data importing is complete, tidb-lightning can automatically perform
    # the Checksum, Compact and Analyze operations. It is recommended to leave
    # these as true in the production environment.
    # The execution order: Checksum -> Compact -> Analyze
    [post-restore]
    # Performs `ADMIN CHECKSUM TABLE <table>` for each table to verify data integrity.
    checksum = true
    # Performs compaction on the TiKV cluster.
    compact = true
    # Performs `ANALYZE TABLE <table>` for each table.
    analyze = true
    ```

4. Run `tidb-lightning`.

    ```sh
    nohup ./tidb-lightning -config tidb-lightning.toml > nohup.out &
    ```
