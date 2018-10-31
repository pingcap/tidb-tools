---
name: "⚡️ Lightning Support"
about: "Requesting support for TiDB Lightning Toolset"

---

## Lightning Support

Please describe your problem here:

>
>
>

Additionally, please provide the following info before submitting your issue. Thanks!

1. Versions of the tools

    - [ ] Lightning version (run `tidb-lightning -V`):

        ```
        (paste Lightning version here)
        ```

    - [ ] Importer version (run `tikv-importer -V`):

        ```
        (paste Importer version here)
        ```

    - [ ] TiDB cluster version (execute `SELECT tidb_version();` in a MySQL client):

        ```
        (paste TiDB cluster version here)
        ```

    - [ ] How did you deploy Lightning and Importer: Manually, or via TiDB Ansible? If it's latter,
        please provide the commit hash when TiDB Ansible was executed.

        ```
        (paste TiDB Ansible version here)
        ```

    - [ ] Other interesting information (system version, hardware config, etc):

        >
        >
        >

2. Operation logs

    - [ ] Please upload `tidb-lightning.log` if possible.
    - [ ] Please upload `tikv-importer.log` if possible.
    - [ ] Please execute `tidb-lightning-ctl --checkpoint-dump=./checkpoints` and upload the `./checkpoints` folder if possible.
    - [ ] Other interesting logs, config files and screenshots.

3. Common issues

    - [ ] Are the addresses of all machines filled in correctly?
    - [ ] Is the `region-concurrency` set to a reasonable value (unset on dedicated machine, 75% of CPU on shared machine)?
    - [ ] Please check if the [common errors] already described a fix?
    - [ ] Please check if the [FAQ] already described a fix?

[common errors]: https://github.com/pingcap/tidb-tools/blob/master/docs/lightning/en_US/05-Errors.md
[FAQ]: https://github.com/pingcap/tidb-tools/blob/master/docs/lightning/en_US/06-FAQ.md
