FAQ
===

### 错误处理

#### SQL 同步出错

TiDB 对部分 DDL 并不支持（或者 DML 执行出错），需要用户根据业务具体情况选择跳过或者改造同步的 DDL。

具体操作方法见 [skip 或 replace 异常 SQL](./exception-handling/skip-replace-sqls.md)

#### Sharding DDL 同步出错

在开启 sharding DDL 同步时，如果自动化的 DDL 同步发生错误，需要使用 dmctl 进行人为干预处理。

主要需要使用的命令包括 `show-ddl-locks`, `unlock-ddl-lock`, `break-ddl-lock`。

但使用上述命令人为干预 sharding DDL 同步前，先阅读 [sharding DDL 使用限制](./shard-table/restrictions.md)。

具体操作见 [手动处理 sharding DDL lock](./shard-table/handle-DDL-lock.md)

#### 重置数据同步任务

某些情况下可能需要重新重置整个数据同步任务，相关情况包括：
- 上游人为执行了 `RESET MASTER`，造成 relay log 同步出错
- relay log 或上游 binlog 损坏或者丢失

一般这时候 relay unit 会发生错误而退出，且无法优雅地自动恢复，需要通过手动方式恢复数据同步。

具体操作方法见 [重置数据同步任务](./exception-handling/reset-task.md)
