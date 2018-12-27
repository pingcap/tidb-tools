skip 或 replace 异常 SQL
===

### 索引

- [功能介绍](#功能介绍)
- [命令介绍](#命令介绍)
- [使用示例](#使用示例)
    - [出错后被动 skip](#出错后被动-skip)
    - [出错前主动 replace](#出错前主动-replace)
    - [sharding 场景下出错前主动 replace](#sharding-场景下出错前主动-replacee)


### 功能介绍

目前 TiDB 并不兼容所有的 MySQL 语法（TiDB 支持的 DDL 语法见 <https://pingcap.com/docs-cn/sql/ddl/>），当使用 DM 从 MySQL 同步数据到 TiDB 时，可能会由于 TiDB 不支持对应 SQL 语句报错而造成同步中断。

当由于 TiDB 不支持的 SQL 导致同步出错时，可以使用 dmctl 来手动选择跳过该 SQL 对应的 binlog event 或使用其它指定的 SQL 来替代本次执行以恢复同步任务。

当提前预知会有 TiDB 不支持的 SQL 将要被同步时，可以使用 dmctl 来手动预定跳过/替代操作，当同步该 SQL 对应的 binlog event 时自动执行预定的操作，避免同步过程被中断。


### 命令介绍

使用 dmctl 手动处理 TiDB 不支持的 SQL 时，主要使用的命令包括 `query-status`、`query-error`、`sql-skip` 与 `sql-replace`。

#### query-status

`query-status` 命令用于查询当前 DM-worker 内子任务及 relay 等的状态，具体参见 [查询数据同步任务状态](../task-handling/task-commands.md#查询数据同步任务状态)。


#### query-error

查询 DM-worker 内子任务及 relay 在运行中当前存在的错误。

##### 命令样例

```bash
query-error [--worker=127.0.0.1:8262] [task-name]
```

##### 参数释解

- `worker`: flag 参数，string，`--worker`，可选；不指定时查询所有 DM-worker 上的错误，指定时仅查询特定的一组 DM-worker 上的错误。
- `task-name`: 非 flag 参数，string，可选；不指定时查询所有任务内的错误，指定时仅查询特定任务内的错误。


#### sql-skip

预定在 binlog event 的 position 或 SQL 与指定的 `binlog-pos` 或 `sql-pattern` 匹配时执行跳过（skip）操作。

##### 命令样例

```bash
sql-skip <--worker=127.0.0.1:8262> [--binlog-pos=mysql-bin|000001.000003:3270] [--sql-pattern=~(?i)ALTER\s+TABLE\s+`db1`.`tbl1`\s+ADD\s+COLUMN\s+col1\s+INT] [--sharding] <task-name>
```

##### 参数解释

- `worker`: flag 参数，string，`--worker`，未指定 `--sharding` 时必选，指定 `--sharding` 时禁止使用；在指定时表示预定操作将生效的 DM-worker。
- `binlog-pos`: flag 参数，string，`--binlog-pos`，与 `--sql-pattern` 必须指定其中一个、且只能指定其中一个；在指定时表示操作将在 binlog event 的 position 匹配时生效，格式为 `binlog-filename:binlog-pos`（比如：`mysql-bin|000001.000003:3270`，在同步已经发生错误时，可通过 `query-erro` 返回的 `failedBinlogPosition` 获得）。
- `sql-pattern`: flag 参数，string，`--sql-pattern`，与 `--binlog-pos` 必须 指定其中一个、且只能指定其中一个；在指定时表示操作将在 binlog event 对应的 DDL 经过可选的 router-rule 转换后匹配时生效，格式为以 `~` 为前缀的正则表达式或完整的待匹配文本（比如：``` ~(?i)ALTER\s+TABLE\s+`db1`.`tbl1`\s+ADD\s+COLUMN\s+col1\s+INT ```，字符串内部不支持空格，如需要使用空格，应使用正则表达式的 `\s+` 替代）。
- `sharding`: flag 参数，boolean，`--sharding`，未指定 `--worker` 时必选，指定 `--worker` 时禁止使用；在指定时表示预定的操作将在 sharding DDL 同步过程中的 owner 内生效。
- `task-name`: 非 flag 参数，string，必选；表示预定的操作将生效的 task。


#### sql-replace

预定在 binlog event 的 position 或 SQL 与指定的 `binlog-pos` 或 `sql-pattern` 匹配时执行替代（replace）操作。

##### 命令样例

```bash
sql-replace <--worker=127.0.0.1:8262> [--binlog-pos=mysql-bin|000001.000003:3270] [--sql-pattern=~(?i)ALTER\s+TABLE\s+`db1`.`tbl1`\s+ADD\s+COLUMN\s+col1\s+INT] [--sharding] <task-name> <SQL-1;SQL-2>
```

##### 参数解释

- `worker`: 与 `sql-skip` 的 `--worker` 一致
- `binlog-pos`: 与 `sql-skip` 的 `--binlog-pos` 一致
- `sql-pattern`: 与 `sql-skip` 的 `--sql-pattern` 一致
- `sharding`: 与 `sql-skip` 的 `--sharding` 一致
- `task-name`: 与 `sql-skip` 的 `task-name` 一致
- `SQLs`: 非 flag 参数，string，必选；表示将用于替代原 binlog event 的新 SQLs，多条 SQL 间以 `;` 分隔（比如：``` ALTER TABLE shard_db.shard_table drop index idx_c2;ALTER TABLE shard_db.shard_table DROP COLUMN c2; ```）


### 使用示例

#### 出错后被动 skip

TODO

#### 出错前主动 replace

TODO

#### sharding 场景下出错前主动 replace

TODO


---


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