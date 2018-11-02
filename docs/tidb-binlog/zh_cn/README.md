# TiDB-Binlog

TiDB-Binlog 是一个用于收集 TiDB 的 Binlog，并提供实时备份和同步功能的商业工具。

TiDB-Binlog 支持以下功能场景:

* **数据同步**：同步 TiDB 集群数据到其他数据库
* **实时备份和恢复**：备份 TiDB 集群数据，同时可以用于 TiDB 集群故障时恢复

## 文档目录

### 1. 用户文档

* [cluster 版本用户文档](./tidb-binlog-cluster.md)

* [kafka 版本用户文档](./kafka/tidb-binlog-kafka.md)

* [local 版本用户文档](./local/tidb-binlog-local.md)

### [2. 监控及告警说明](./tidb-binlog-monitor.md)

### [3. slave client 用户文档](./binlog-slave-client.md)

### [4. 问题反馈及排查流程](./tidb-binlog-issue.md)