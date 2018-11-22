skip 或 replace 异常 SQL
===

当 binlog replication/syncer 在执行 SQL (DDL / DML) 出错时，DM 支持通过 dmctl 手动选择跳过该条 SQL 或以其他用户指定的 SQL 替代本次执行。

手动处理出错 SQL 时，使用的主要命令包括 `query-status`, `sql-skip`, `sql-replace`。

### 手动处理异常 SQL 流程

1. 使用 `query-status` 查询任务当前运行状态
    1. 是否由于遇到错误而导致某个 DM-worker 的某个任务状态为 `Paused`
    2. 任务出错的原因是否是执行 SQL 出错
2. 记录下 `query-status` 时返回的 syncer 已同步的 binlog pos (`SyncerBinlog`)
3. 根据出错情况及业务场景等，决定是否要 跳过 / 替代 当前出错 SQL
4. 如果需要跳过当前出错 SQL
    * 使用 `sql-skip` 指定需要执行 SQL 跳过操作的 DM-worker, task name 及 binlog pos 并执行跳过操作
5. 如果需要替代执行当前出错 SQL
    * 使用 `sql-replace` 指定需要执行 SQL 替代操作的 DM-worker, task name, binlog pos 及 用于替代原 SQL 的新 SQL（可指定多条，`;` 分隔）
6. 使用 `resume-task` 并指定 DM-worker, task name 恢复由于出错而 paused 的 DM-worker 上的任务
7. 使用 `query-status` 查看 SQL 跳过是否成功

#### 如何找到参数中需要指定的 binlog pos

在 `dm-worker.log` 中查找对应于出错 SQL 的 `current pos`


### 命令参数说明

#### sql-skip

- `worker`: flag 参数，string，`--worker`，必选；指定需要执行跳过操作的 SQL 所处的 DM-worker
- `task-name`: 非 flag 参数，string，必选；指定需要执行跳过操作的 SQL 所处的 task
- `binlog-pos`: 非 flag 参数，string，必选；指定需要执行跳过操作的 SQL 所处的 binlog pos，形式为 `mysql-bin.000002:123`（`:` 分隔 binlog name 与 pos）

#### sql-replace

- `worker`: flag 参数，string，`--worker`，必选；指定需要执行替代操作的 SQL 所处的 DM-worker
- `task-name`: 非 flag 参数，string，必选；指定需要执行替代操作的 SQL 所处的 task
- `binlog-pos`: 非 flag 参数，string，必选；指定需要执行替代操作的 SQL 所处的 binlog pos，形式为 `mysql-bin.000002:123`（`:` 分隔 binlog name 与 pos）
- `sqls`：非 flag 参数，string，必选；指定将用于替代原 SQL 的新 SQLs（可指定多条，`;` 分隔）