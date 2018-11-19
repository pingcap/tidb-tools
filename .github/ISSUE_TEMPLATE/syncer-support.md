---
name: "syncer Support"
about: "Requesting support for syncer Tool"

---

## syncer Support

Before submitting your issue, please check the requirements list below (all need to be met):

1. Upstream MySQL version (execute `SELECT @@version;` in a MySQL client)
    - MySQL: 5.6 <= version < 5.7
    - MariaDB: version >= 10.1.2 
2. Upstream MySQL `server_id` (execute `SHOW VARIABLES LIKE 'server_id';` in a MySQL client)
    - 0 < server_id < 4294967295
    - also needs to be unique
3. Upstream MySQL user privileges
    - SELECT
    - REPLICATION SLAVE
    - REPLICATION CLIENT
4. Downstream TiDB user privileges
    - SELECT
    - INSERT
    - UPDATE
    - DELETE
    - CREATE
    - DROP
    - ALTER
    - INDEX
5. Upstream MySQL variables for binlog
    - `SHOW GLOBAL VARIABLES LIKE 'log_bin';`: `ON`
    - `SHOW GLOBAL VARIABLES LIKE 'binlog_format';`: `ROW`  
        if you start to sync from an older binlog pos, you must ensure the `binlog_format` from that pos is also `ROW`.
    - for MySQL >= 5.6.2 or MariaDB >= 10.1.6, `SHOW GLOBAL VARIABLES LIKE 'binlog_row_image';`: `FULL`


Please describe your problem here:

>
>
>

Additionally, please provide the following info before submitting your issue. Thanks!

1. Versions of the tools

    - [ ] syncer version (run `syncer -V`):

        ```
        (paste syncer version here)
        ```

    - [ ] Upstream MySQL server version (execute `SELECT @@version;` in a MySQL client):

        ```
        (paste upstream MySQL server version here)
        ```

    - [ ] Downstream TiDB cluster version (execute `SELECT tidb_version();` in a MySQL client):

        ```
        (paste downstream TiDB cluster version here)
        ```

    - [ ] Upstream MySQL sql_mode (execute `SELECT @@sql_mode;` in a MySQL client):

        ```
        (paste MySQL sql_mode here)
        ```

    - [ ] Downstream TiDB sql_mode (execute `SELECT @@sql_mode;` in a MySQL client):

        ```
        (paste TiDB sql_mode here)
        ```

    - [ ] How did you deploy syncer?

        ```
        ```

    - [ ] Other interesting information (table schema, system version, hardware config, etc):

        >
        >
        >

2. Operation logs
    - [ ] please provide config file of syncer.
    - [ ] Please upload `syncer.log` if possible.
    - [ ] Please upload monitor screenshots. if not deployed, please add prometheus monitor and [grafana dashboard](https://github.com/pingcap/tidb-ansible/blob/master/scripts/syncer.json)
    - [ ] Other interesting logs

3. Common issues
    - [ ] Is the `worker-count` set to a reasonable value (suggest-32)?
    - [ ] Is the `batch` set to a reasonable value (suggest 100-1000)?
    - [ ] Is the downstream server MySQL and does not use the `utf8mb4` charset (syncer only allows the downstream server's charset to be `utf8mb4`)?
    - [ ] Do not some tables have primary/unique keys and slow synchronization? Syncer is not friendly for tables don't have priamry/unique key.