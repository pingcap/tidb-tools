TiDB Lightning
==============

**TiDB Lightning** is a tool for fast full import of large amounts of data into a TiDB cluster.
Currently, we support reading SQL dump exported via mydumper.

1. [Architecture](01-Architecture.md)
2. [Deployment and Execution](02-Deployment.md)
3. [Using Checkpoints](03-Checkpoints.md)
4. [Metrics](04-Metrics.md)
5. [Common Errors](05-Errors.md)
6. [FAQ](06-FAQ.md)

## Notes

Before starting TiDB Lightning, note that:

- During the import process, the cluster cannot provide normal services.
- If `tidb-lightning` crashes, the cluster will be left in "import mode".
    Forgetting to switch back to "normal mode" will lead to a high amount of uncompacted data on
    the TiKV cluster, and will cause abnormally high CPU usage and stall.
    You can manually switch the cluster back to "normal mode" via the `tidb-lightning-ctl` tool:

    ```sh
    bin/tidb-lightning-ctl -switch-mode=normal
    ```
