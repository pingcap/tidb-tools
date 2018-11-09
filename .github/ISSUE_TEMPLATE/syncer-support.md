---
name: "syncer Support"
about: "Requesting support for syncer Tool"

---

## syncer Support

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

    - [ ] TiDB cluster version (execute `SELECT tidb_version();` in a MySQL client):

        ```
        (paste TiDB cluster version here)
        ```

    - [ ] How did you deploy syncer?

        ```
        ```

    - [ ] Other interesting information (system version, hardware config, etc):

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
    - [ ] Is the downstream server mysql and using multiStatements (syncer doesn't provide multiStatements)?
    - [ ] Is the downstream server mysql and does not use the utf8mb4 character set charset (syncer only allows the downstream server's character set to be utf8mb4)?