---
name: "DM Support"
about: "Requesting support for DM (Data Migration)"

---

## DM Support

Please describe your problem here:

>
>
>

Additionally, please provide the following info before submitting your issue. Thanks!

1. Versions of the tools

    - [ ] DM version (run `dmctl -V` or `dm-worker -V` or `dm-master -V`):

        ```
        (paste DM version here, and your must ensure versions of dmctl, dm-worker and dm-master are the same)
        ```

    - [ ] Upstream MySQL / MariaDB server version:

        ```
        (paste upstream MySQL / MariaDB server version here)
        ```

    - [ ] Downstream TiDB cluster version (execute `SELECT tidb_version();` in a MySQL client):

        ```
        (paste TiDB cluster version here)
        ```

    - [ ] How did you deploy DM: manually, or via dm-ansible?.

        ```
        (leave manually or dm-ansible here)
        ```

    - [ ] Other interesting information (system version, hardware config, etc):

        >
        >
        >

2. current status of DM cluster (execute `query-status` in dmctl)

3. Operation logs

    - [ ] Please upload `dm-worker.log` for every dm-worker instance if possible.
    - [ ] Please upload `dm-master.log` if possible.
    - [ ] Other interesting logs.
    - [ ] Output of dmctl's commands with problems.

4. Configuration of the cluster and the task

    - [ ] `dm-worker.toml` for every dm-worker instance if possible.
    - [ ] `dm-master.toml` for dm-master if possible.
    - [ ] task config, like `task.yaml` if possible.
    - [ ] `inventory.ini` if deployed by dm-ansible.

5. Screenshot of Grafana dashboard or metrics' graph in Prometheus for DM if possible
