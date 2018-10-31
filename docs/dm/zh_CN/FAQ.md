FAQ
===

### 错误处理

#### SQL 同步出错

TiDB 对部分 DDL 并不支持（或者 DML 执行出错），需要用户根据业务具体情况选择跳过或者改造同步的 DDL。

具体操作方法见 [错误处理/skip/replace 异常 SQL]

#### Sharding DDL 同步出错

在开启 sharding DDL 同步时，如果自动化的 DDL 同步发生错误，需要使用 dmctl 进行人为干预处理。

主要需要使用的命令包括 `show-ddl-locks`, `unlock-ddl-lock`, `break-ddl-lock`。

但使用上述命令人为干预 sharding DDL 同步前，请确保已了解 sharding DDL 同步的原理及当前支持人为干预处理的错误类型及可能造成的影响。

具体操作见 [分库分表/分库分表数据同步]

#### 重置数据同步任务

某些情况下需要重新重置数据同步任务，一般情况是上游数据库 binlog 损坏或者丢失，包括：
- 人为执行了 `RESET MASTER`
- binlog 损坏或者丢失

一般这时候 relay unit 会发生错误而退出，且无法优雅地自动恢复（后续会完善 relay 的错误处理、恢复机制）时，目前需要通过手动全量恢复数据同步。

具体操作方法见 [错误处理/重置数据同步任务]
