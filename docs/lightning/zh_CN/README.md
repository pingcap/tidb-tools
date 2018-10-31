TiDB Lightning
==============

**TiDB Lightning** 是一个全量数据高速导入到 TiDB 集群的工具。目前支持 mydumper 输出格式的数据源。

1. [软件架构](01-Architecture.md)
2. [部署执行](02-Deployment.md)
3. [断点续传](03-Checkpoints.md)
4. [监控告警](04-Metrics.md)
5. [错误排解](05-Errors.md)
6. [常见问题](06-FAQ.md)

## 注意事项

在使用 TiDB Lightning 前，请注意：

- TiDB Lightning 运行后，TiDB 集群将无法正常对外提供服务。
- 若 `tidb-lightning` 崩溃，集群会留在“导入模式”。若忘记转回“普通模式”，集群会产生大量未压缩的文件，
    继而消耗 CPU 并导致迟延 (stall)。此时需要使用 `tidb-lightning-ctl` 手动把集群转回“普通模式”：

    ```sh
    bin/tidb-lightning-ctl -switch-mode=normal
    ```
