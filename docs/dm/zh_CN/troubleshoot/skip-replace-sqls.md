skip 或 replace 异常 SQL
===

### 索引

- [功能介绍](#功能介绍)
- [支持场景](#支持场景)
- [实现原理](#实现原理)
    - [如何匹配 binlog event](#如何匹配-binlog-event)
- [命令介绍](#命令介绍)
- [使用示例](#使用示例)
    - [出错后被动 skip](#出错后被动-skip)
    - [出错前主动 replace](#出错前主动-replace)
    - [合库合表场景下出错前主动 replace](#合库合表场景下出错前主动-replace)


### 功能介绍

目前 TiDB 并不兼容所有的 MySQL 语法（TiDB 支持的 DDL 语法见 <https://pingcap.com/docs-cn/sql/ddl/>），当使用 DM 从 MySQL 同步数据到 TiDB 时，可能会由于 TiDB 不支持对应 SQL 语句报错而造成同步中断。

当由于 TiDB 不支持的 SQL 导致同步出错时，可以使用 dmctl 来手动选择跳过该 SQL 对应的 binlog event 或使用其它指定的 SQL 来替代对应的 binlog event 向下游执行以恢复同步任务。

当提前预知会有 TiDB 不支持的 SQL 将要被同步时，也可以使用 dmctl 来手动预设跳过/替代操作，当尝试同步该 SQL 对应的 binlog event 到下游时自动执行预设的操作，避免同步过程被中断。

注意：
- skip 或 replace 只适合用于跳过/替代执行 **下游 TiDB 不支持执行的 SQL**，其它同步错误请不要使用此方式进行处理
    - 其它同步错误可尝试使用 [同步表黑白名单](../features/black-white-list.md) 或 [binlog 过滤](../features/binlog-filter.md)
- 如果业务不能接受下游 TiDB 不执行出错的 DDL，且也不能使用其它 DDL 作为替代，则不适合使用本功能
    - 比如：DROP PRIMARY KEY
    - 此时只能在下游重建（DDL 执行完后的）新表结构对应的表，并完整重导该表的全部数据
- 单次 skip 或 replace 操作都是针对单个 binlog event 的
- `--sharding` 操作都是针对 sharding DDL lock 的 owner 的，且此时只能使用 `--sql-pattern` 来进行匹配
- `--sharding` 操作必须在上游全部分表执行完 DDL 之前使用，否则无法生效或可能被错误地应用于后续的其它 DDL
    - 即应该先使用 `sql-skip` / `sql-replace` 预设跳过/替代执行操作后后，再在上游（至少最后一个分表）执行 DDL


### 支持场景

- 同步过程中，上游执行了 TiDB 不支持的 DDL 并同步到了 DM 造成了同步任务中断
    - 业务能接受下游 TiDB 不执行该 DDL，使用 `sql-skip` 跳过对该 DDL 的同步以恢复同步任务
    - 业务能接受下游 TiDB 执行其它 DDL 来作为替代，使用 `sql-replace` 替代该 DDL 的同步以恢复同步任务
- 同步过程中，预先知道上游将执行 TiDB 不支持的 DDL，提前处理以避免同步任务中断
    - 业务能接受下游 TiDB 不执行该 DDL，使用 `sql-skip` 预设一个跳过 DDL 的操作，当执行到该 DDL 时即自动跳过
    - 业务能接受下游 TiDB 执行其它 DDL 来作为替代，使用 `sql-replace` 预设一个替代 DDL 的操作，当执行到该 DDL 时即自动替代


### 实现原理

DM 在进行增量数据同步时，简化后的流程大致可表述为：

1. relay 处理单元从上游拉取 binlog 存储在本地作为 relay log
2. replicate binlog（sync）处理单元读取本地 relay log，获取其中的 binlog event
3. 从 binlog event 中解析、构造 DDL/DML 后向下游 TiDB 执行

在解析 binlog event 向下游 TiDB 执行时，可能会由于 TiDB 不支持对应的 SQL 报错而造成同步中断。

在 DM 中，可以为 binlog event 注册一些跳过（skip）/替代执行（replace）操作（operator）。在向下游 TiDB 执行 SQL 前，将当前的 binlog event 信息（position、DDL）与注册的 operator 进行比较。如果 position 或 DDL 与注册的某个 operator 匹配，则执行该 operator 对应的操作并将该 operator 移除。

**同步中断后使用 `sql-skip` / `sql-replace` 恢复任务的流程**

1. 使用 `sql-skip` / `sql-replace` 为指定的 binlog position 或 DDL pattern 注册 operator
2. 使用 `resume-task` 恢复之前由于同步出错导致中断的任务
3. 重新解析获得之前造成同步出错的 binlog event
4. 该 binlog event 与 step.1 时注册的 operator 匹配成功
5. 执行 operator 对应的操作（skip/replace）后，继续执行同步任务

**同步中断前使用 `sql-skip` / `sql-replace` 避免任务中断的流程**

1. 使用 `sql-skip` / `sql-replace` 为指定的 DDL pattern 注册 operator
2. 从 relay log 中解析获得 binlog event
3. （包含 TiDB 不支持 SQL 的）binlog event 与 step.1 时注册的 operator 匹配成功
4. 执行 operator 对应的操作（skip/replace）后，继续执行同步任务，不发生中断

**合库合表同步中断前使用 `sql-skip` / `sql-replace` 避免任务中断的流程**

1. 使用 `sql-skip` / `sql-replace` 为指定的 DDL pattern 注册 operator（在 DM-master 上）
2. 各 DM-worker 从 relay log 中解析获得 binlog event
3. 各 DM-worker 通过 DM-master 协调进行 DDL lock 同步
4. DM-master 判断 DDL lock 同步成功，将 step.1 时注册的 operator 发送给 DDL lock owner
5. DM-master 请求 DDL lock owner 执行 DDL
6. DDL lock owner 将要执行的 DDL 与 step.4 收到的 operator 匹配成功
7. 执行 operator 对应的操作（skip/replace）后，继续执行同步任务


#### 如何匹配 binlog event

当同步任务由于执行 SQL 出错而中断时，可以使用 `query-error` 获取对应 binlog event 的 position 信息。通过在 `sql-skip` / `sql-replace` 执行时指定该 position 信息，即可与对应的 binlog event 进行匹配。

但当需要在同步中断前主动处理 SQL 不被支持的情况以避免同步任务中断时，由于无法提前预知 binlog event 的 position 信息，因此需要使用其它方式来确保与后续将到达的 binlog event 进行匹配。

在 DM 中，支持如下两种方式的 binlog event 匹配模式（两种模式只能二选其一）：
1. binlog position: binlog event 在 binlog file 中的起始位置，使用 `SHOW BINLOG EVENTS` 时输出的 `End_log_pos`（不是 `Pos`）
2. DDL pattern: （仅限于 DDL 的）正则表达式匹配模式，以 `~` 为前缀，不包含原始空格（字符串中空格以 `\s` 或 `\s+` 表示）

对于合库合表场景，如果需要由 DM 自动选择 DDL lock owner 来执行跳过/替代执行操作，则由于不同 DM-worker 上 DDL 对应的 binlog position 无逻辑关联且难以确定，因此只能使用 DDL pattern 匹配模式。


**限制：**

- 一个 binlog event 只能注册一个 operator，后注册的 operator 会覆盖之前已经注册的 operator
- operator 在与 binlog event 匹配成功后即会被删除，后续如果需要再进行（`--sql-pattern`）匹配需要重新注册


### 命令介绍

使用 dmctl 手动处理 TiDB 不支持的 SQL 时，主要使用的命令包括 `query-status`、`query-error`、`sql-skip` 与 `sql-replace`。

#### query-status

`query-status` 命令用于查询当前 DM-worker 内子任务及 relay 等的状态，具体参见 [查询数据同步任务状态](../task-handling/task-commands.md#查询数据同步任务状态)。


---

#### query-error

查询 DM-worker 内子任务及 relay 在运行中当前存在的错误。

##### 命令样例

```bash
query-error [--worker=127.0.0.1:8262] [task-name]
```

##### 参数释解

- `worker`: flag 参数，string，`--worker`，可选；不指定时查询所有 DM-worker 上的错误，指定时仅查询特定的一组 DM-worker 上的错误
- `task-name`: 非 flag 参数，string，可选；不指定时查询所有任务内的错误，指定时仅查询特定任务内的错误


---

#### sql-skip

预设在 binlog event 的 position 或 SQL 与指定的 `binlog-pos` 或 `sql-pattern` 匹配时执行该跳过（skip）操作。

##### 命令样例

```bash
sql-skip <--worker=127.0.0.1:8262> [--binlog-pos=mysql-bin|000001.000003:3270] [--sql-pattern=~(?i)ALTER\s+TABLE\s+`db1`.`tbl1`\s+ADD\s+COLUMN\s+col1\s+INT] [--sharding] <task-name>
```

##### 参数解释

- `worker`: flag 参数，string，`--worker`，未指定 `--sharding` 时必选，指定 `--sharding` 时禁止使用；在指定时表示预设操作将生效的 DM-worker
- `binlog-pos`: flag 参数，string，`--binlog-pos`，与 `--sql-pattern` 必须指定其中一个、且只能指定其中一个；在指定时表示操作将在 binlog event 的 position 匹配时生效，格式为 `binlog-filename:binlog-pos`（比如：`mysql-bin|000001.000003:3270`，在同步已经发生错误时，可通过 `query-error` 返回的 `failedBinlogPosition` 获得）
- `sql-pattern`: flag 参数，string，`--sql-pattern`，与 `--binlog-pos` 必须指定其中一个、且只能指定其中一个；在指定时表示操作将在 binlog event 对应的 DDL 经过可选的 router-rule 转换后匹配时生效，格式为以 `~` 为前缀的正则表达式（比如：``` ~(?i)ALTER\s+TABLE\s+`db1`.`tbl1`\s+ADD\s+COLUMN\s+col1\s+INT ```，字符串内部不支持空格，如需要使用空格，应使用正则表达式的 `\s+` 替代）
    - 注意：
        - 使用正则表达式时，以 `~` 为前缀，语法参考 <https://golang.org/pkg/regexp/syntax/#hdr-Syntax>
        - 正则中的 schema 和 table 名必须是经过可选的 router-rule 转换后的名字，即对应下游的 target schema / table 名。如上游为 ``` `shard_db_1`.`shard_tbl_1` ```，下游为 ``` `shard_db`.`shard_tbl` ```，则应该尝试匹配 ``` `shard_db`.`shard_tbl` ```
        - 正则中的 schema 名、table 名及 column 名需要使用 ``` ` ``` 标记，如：``` `db1`.`tbl1` ```
        - 暂时不支持正则表达式中包含原始的空格，需要使用 `\s` 或 `\s+` 替代空格
- `sharding`: flag 参数，boolean，`--sharding`，未指定 `--worker` 时必选，指定 `--worker` 时禁止使用；在指定时表示预设的操作将在 sharding DDL 同步过程中的 DDL lock owner 内生效
- `task-name`: 非 flag 参数，string，必选；表示预设的操作将生效的 task


---

#### sql-replace

预设在 binlog event 的 position 或 SQL 与指定的 `binlog-pos` 或 `sql-pattern` 匹配时执行该替代（replace）操作。

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

##### 业务场景

假设当前需要同步上游的 `db1.tbl1` 表到下游 TiDB（非合库合表同步），初始时表结构为：

```sql
mysql> SHOW CREATE TABLE db1.tbl1;
+-------+--------------------------------------------------+
| Table | Create Table                                     |
+-------+--------------------------------------------------+
| tbl1  | CREATE TABLE `tbl1` (
  `c1` int(11) NOT NULL,
  `c2` decimal(11,3) DEFAULT NULL,
  PRIMARY KEY (`c1`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 |
+-------+--------------------------------------------------+
```

如果此时上游执行以下 DDL 修改表结构（即将列的 DECIMAL(11, 3) 修改为 DECIMAL(10, 3)）：

```sql
ALTER TABLE db1.tbl1 CHANGE c2 c2 DECIMAL (10, 3);
```

则会由于 TiDB 不支持该 DDL 而导致 DM 同步任务中断且报如下错误：

```bash
exec sqls[[USE `db1`; ALTER TABLE `db1`.`tbl1` CHANGE COLUMN `c2` `c2` decimal(10,3);]] failed, err:Error 1105: unsupported modify column length 10 is less than origin 11
```

此时使用 `query-status` 查询任务状态，可看到 `stage` 转为了 `Paused` 且 `errors` 中有相关的错误描述信息。

使用 `query-error` 可以更明确地获取到该错误的信息，如下：

```bash
» query-error test
{
    "result": true,
    "msg": "",
    "workers": [
        {
            "result": true,
            "worker": "127.0.0.1:8262",
            "msg": "",
            "subTaskError": [
                {
                    "name": "test",
                    "stage": "Paused",
                    "unit": "Sync",
                    "sync": {
                        "errors": [
                            {
                                "msg": "exec sqls[[USE `db1`; ALTER TABLE `db1`.`tbl1` CHANGE COLUMN `c2` `c2` decimal(10,3);]] failed, err:Error 1105: unsupported modify column length 10 is less than origin 11",
                                "failedBinlogPosition": "mysql-bin|000001.000003:34642",
                                "errorSQL": "[USE `db1`; ALTER TABLE `db1`.`tbl1` CHANGE COLUMN `c2` `c2` decimal(10,3);]"
                            }
                        ]
                    }
                }
            ],
            "RelayError": {
                "msg": ""
            }
        }
    ]
}
```

##### 被动 skip 该 SQL

假设业务上可以接受下游 TiDB 不执行此 DDL（即继续保持原有的表结构），则可以通过使用 `sql-skip` 命令跳过该 DDL 以恢复任务同步。操作步骤如下：

1. 使用 `query-error` 获取同步出错的 binlog event position 信息
    - 即上面 `query-error` 返回信息中的 `failedBinlogPosition` 信息
    - 本示例中为 `mysql-bin|000001.000003:34642`
2. 使用 `sql-skip` 命令预设一个 binlog event 跳过（skip）操作（该操作将在 `resume-task` 后同步该 binlog event 到下游时生效）
    ```bash
    » sql-skip --worker=127.0.0.1:8262 --binlog-pos=mysql-bin|000001.000003:34642 test
    {
        "result": true,
        "msg": "",
        "workers": [
            {
                "result": true,
                "worker": "",
                "msg": ""
            }
        ]
    }
    ```
    对应 DM-worker 节点中也可以看到类似如下 log：
    ```bash
    2018/12/28 11:17:51 operator.go:136: [info] [sql-operator] set a new operator uuid: 6bfcf30f-2841-4d70-9a34-28d7082bdbd7, pos: (mysql-bin|000001.000003, 34642), op: SKIP, args:
    ```
3. 使用 `resume-task` 恢复之前出错中断的 task
    ```bash
    » resume-task --worker=127.0.0.1:8262 test
    {
        "op": "Resume",
        "result": true,
        "msg": "",
        "workers": [
            {
                "op": "Resume",
                "result": true,
                "worker": "127.0.0.1:8262",
                "msg": ""
            }
        ]
    }
    ```
    对应 DM-worker 节点中也可以看到类似如下 log：
    ```bash
    2018/12/28 11:27:46 operator.go:173: [info] [sql-operator] binlog-pos (mysql-bin|000001.000003, 34642) matched, applying operator uuid: 6bfcf30f-2841-4d70-9a34-28d7082bdbd7, pos: (mysql-bin|000001.000003, 34642), op: SKIP, args:
    ```
4. 使用 `query-status` 确认任务 `stage` 已经转为 `Running`
5. 使用 `query-error` 确认原错误信息已经不存在


---

#### 出错前主动 replace

##### 业务场景

假设当前需要同步上游的 `db2.tbl2` 表到下游 TiDB（非合库合表同步），初始时表结构为：

```sql
mysql> SHOW CREATE TABLE db2.tbl2;
+-------+--------------------------------------------------+
| Table | Create Table                                     |
+-------+--------------------------------------------------+
| tbl2  | CREATE TABLE `tbl2` (
  `c1` int(11) NOT NULL,
  `c2` int(11) DEFAULT NULL,
  PRIMARY KEY (`c1`),
  KEY `idx_c2` (`c2`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 |
+-------+--------------------------------------------------+
```

如果此时上游执行以下 DDL 修改表结构（即 DROP 列 c2）：

```sql
ALTER TABLE db2.tbl2 DROP COLUMN c2;
```

假设使用 DM 进行同步，则当同步该 DDL 对应的 binlog event 到下游时，会由于 TiDB 不支持该 DDL 而导致 DM 同步任务中断且报如下错误：

```bash
exec sqls[[USE `db2`; ALTER TABLE `db2`.`tbl2` DROP COLUMN `c2`;]] failed, err:Error 1105: can't drop column c2 with index covered now
```

**但如果我们在上游实际执行该 DDL 前，已经知道该 DDL 不被 TiDB 所支持。** 则我们可以使用 `sql-skip` / `sql-replace` 为此 DDL 提前预设一个跳过（skip）/替代执行（replace）操作。

对于这个示例业务中的 DDL，由于 TiDB 暂时不支持 DROP 存在索引的列，因此我们使用两条 SQLs 来替代执行，即可以先 DROP 索引、然后再 DROP c2 列。

##### 主动 replace 该 SQL

1. 为将要在上游执行的 DDL（经过可选的 router-rule 转换后的 DDL）设计一个可以匹配上的正则表达式
    - 上游将执行的 DDL 为 `ALTER TABLE db2.tbl2 DROP COLUMN c2;`
    - 由于不存在 router-rule 转换，因此可设计正则表达式
        ```sql
        ~(?i)ALTER\s+TABLE\s+`db2`.`tbl2`\s+DROP\s+COLUMN\s+`c2`
        ```
2. 为该 DDL 设计将用于替代执行的新 DDLs
    ```sql
    ALTER TABLE `db2`.`tbl2` DROP INDEX idx_c2;ALTER TABLE `db2`.`tbl2` DROP COLUMN `c2`
    ```
3. 使用 `sql-replace` 命令预设一个 binlog event 替代执行（replace）操作（该操作将在同步该 binlog event 到下游时生效）
    ```bash
    » sql-replace --worker=127.0.0.1:8262 --sql-pattern=~(?i)ALTER\s+TABLE\s+`db2`.`tbl2`\s+DROP\s+COLUMN\s+`c2` test ALTER TABLE `db2`.`tbl2` DROP INDEX idx_c2;ALTER TABLE `db2`.`tbl2` DROP COLUMN `c2`
    {
        "result": true,
        "msg": "",
        "workers": [
            {
                "result": true,
                "worker": "",
                "msg": ""
            }
        ]
    }
    ```
    对应 DM-worker 节点中也可以看到类似如下 log：
    ```bash
    2018/12/28 15:33:13 operator.go:136: [info] [sql-operator] set a new operator uuid: c699a18a-8e75-47eb-8e7e-0e5abde2053c, pattern: ~(?i)ALTER\s+TABLE\s+`db2`.`tbl2`\s+DROP\s+COLUMN\s+`c2`, op: REPLACE, args: ALTER TABLE `db2`.`tbl2` DROP INDEX idx_c2; ALTER TABLE `db2`.`tbl2` DROP COLUMN `c2`
    ```
4. 在上游 MySQL 执行 DDL
5. 观察下游表结构是否变更成功，对应 DM-worker 节点中也可以看到类似如下 log：
    ```bash
    2018/12/28 15:33:45 operator.go:173: [info] [sql-operator] sql-pattern ~(?i)ALTER\s+TABLE\s+`db2`.`tbl2`\s+DROP\s+COLUMN\s+`c2` matched SQL USE `db2`; ALTER TABLE `db2`.`tbl2` DROP COLUMN `c2`;, applying operator uuid: c699a18a-8e75-47eb-8e7e-0e5abde2053c, pattern: ~(?i)ALTER\s+TABLE\s+`db2`.`tbl2`\s+DROP\s+COLUMN\s+`c2`, op: REPLACE, args: ALTER TABLE `db2`.`tbl2` DROP INDEX idx_c2; ALTER TABLE `db2`.`tbl2` DROP COLUMN `c2`
    ```
6. 使用 `query-status` 确认任务 `stage` 持续为 `Running`
7. 使用 `query-error` 确认不存在 DDL 执行错误


---

#### 合库合表场景下出错前主动 replace

##### 业务场景

假设当前存在如下的 4 个上游表需要合并后同步到下游的同一个表 ``` `shard_db`.`shard_table` ```：
- MySQL 实例 1 内有 `shard_db_1` 逻辑库，包括 `shard_table_1` 和 `shard_table_2` 两个表
- MySQL 实例 2 内有 `shard_db_2` 逻辑库，包括 `shard_table_1` 和 `shard_table_2` 两个表

初始时表结构为：

```sql
mysql> SHOW CREATE TABLE shard_db_1.shard_table_1;
+---------------+------------------------------------------+
| Table         | Create Table                             |
+---------------+------------------------------------------+
| shard_table_1 | CREATE TABLE `shard_table_1` (
  `c1` int(11) NOT NULL,
  `c2` int(11) DEFAULT NULL,
  PRIMARY KEY (`c1`),
  KEY `idx_c2` (`c2`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 |
+---------------+------------------------------------------+
```

如果此时在上游所有分表上都执行以下 DDL 修改表结构（即 DROP 列 c2）：

```sql
ALTER TABLE shard_db_*.shard_table_* DROP COLUMN c2;
```

则当 DM 通过 sharding DDL lock 协调 2 个 DM-worker 同步该 DDL、请求 DDL lock owner 向下游 TiDB 执行该 DDL 时，会由于 TiDB 不支持该 DDL 而导致 DM 同步任务中断且报如下错误：

```bash
exec sqls[[USE `shard_db`; ALTER TABLE `shard_db`.`shard_table` DROP COLUMN `c2`;]] failed, err:Error 1105: can't drop column c2 with index covered now
```

**但如果我们在上游实际执行该 DDL 前，已经知道该 DDL 不被 TiDB 所支持。** 则我们可以使用 `sql-skip` / `sql-replace` 为此 DDL 提前预设一个跳过（skip）/替代执行（replace）操作。

对于这个示例业务中的 DDL，由于 TiDB 暂时不支持 DROP 存在索引的列，因此我们使用两条 SQLs 来替代执行，即可以先 DROP 索引、然后再 DROP c2 列。

##### 主动 replace 该 SQL

1. 为将要在上游执行的 DDL（经过可选的 router-rule 转换后的 DDL）设计一个可以匹配上的正则表达式
    - 上游将执行的 DDL 为 `ALTER TABLE shard_db_*.shard_table_* DROP COLUMN c2`;
    - 由于存在 router-rule 会将表名转为 ``` `shard_db`.`shard_table` ```，因此可设计正则表达式
        ```sql
        ~(?i)ALTER\s+TABLE\s+`shard_db`.`shard_table`\s+DROP\s+COLUMN\s+`c2`
        ```
2. 为该 DDL 设计将用于替代执行的新 DDL
    ```sql
    ALTER TABLE `shard_db`.`shard_table` DROP INDEX idx_c2;ALTER TABLE `shard_db`.`shard_table` DROP COLUMN `c2`
    ```
3. 这是合库合表场景，因此使用 `--sharding` 参数来由 DM 自动确定替代执行操作只发生在 DDL lock 的 owner 上
4. 使用 `sql-replace` 命令预设一个 binlog event 替代执行（replace）操作（该操作将在同步该 binlog event 到下游时生效）
    ```bash
    » sql-replace --sharding --sql-pattern=~(?i)ALTER\s+TABLE\s+`shard_db`.`shard_table`\s+DROP\s+COLUMN\s+`c2` test ALTER TABLE `shard_db`.`shard_table` DROP INDEX idx_c2;ALTER TABLE `shard_db`.`shard_table` DROP COLUMN `c2`
     {
         "result": true,
         "msg": "request with --sharding saved and will be sent to DDL lock's owner when resolving DDL lock",
         "workers": [
         ]
     }
    ```
    **DM-master** 节点中也可以看到类似如下 log：
    ```bash
    2018/12/28 16:53:33 operator.go:108: [info] [sql-operator] set a new operator uuid: eba35acd-6c5e-4bc3-b0b0-ae8bd1232351, request: name:"test" op:REPLACE args:"ALTER TABLE `shard_db`.`shard_table` DROP INDEX idx_c2;" args:"ALTER TABLE `shard_db`.`shard_table` DROP COLUMN `c2`" sqlPattern:"~(?i)ALTER\\s+TABLE\\s+`shard_db`.`shard_table`\\s+DROP\\s+COLUMN\\s+`c2`" sharding:true
    ```
5. 在上游 MySQL 实例的分表上执行 DDL
6. 观察下游表结构是否变更成功，对应的 DDL lock **owner** 节点中也可以看到类型如下的 log：
    ```bash
    2018/12/28 16:54:35 operator.go:136: [info] [sql-operator] set a new operator uuid: c959f2fb-f1c2-40c7-a1fa-e73cd51736dd, pattern: ~(?i)ALTER\s+TABLE\s+`shard_db`.`shard_table`\s+DROP\s+COLUMN\s+`c2`, op: REPLACE, args: ALTER TABLE `shard_db`.`shard_table` DROP INDEX idx_c2; ALTER TABLE `shard_db`.`shard_table` DROP COLUMN `c2`
    ```
    ```bash
    2018/12/28 16:54:35 operator.go:173: [info] [sql-operator] sql-pattern ~(?i)ALTER\s+TABLE\s+`shard_db`.`shard_table`\s+DROP\s+COLUMN\s+`c2` matched SQL USE `shard_db`; ALTER TABLE `shard_db`.`shard_table` DROP COLUMN `c2`;, applying operator uuid: c959f2fb-f1c2-40c7-a1fa-e73cd51736dd, pattern: ~(?i)ALTER\s+TABLE\s+`shard_db`.`shard_table`\s+DROP\s+COLUMN\s+`c2`, op: REPLACE, args: ALTER TABLE `shard_db`.`shard_table` DROP INDEX idx_c2; ALTER TABLE `shard_db`.`shard_table` DROP COLUMN `c2`
    ```
    同时，**DM-master** 节点中也可以看到类似如下 log：
    ```bash
    2018/12/28 16:54:35 operator.go:125: [info] [sql-operator] get an operator uuid: eba35acd-6c5e-4bc3-b0b0-ae8bd1232351, request: name:"test" op:REPLACE args:"ALTER TABLE `shard_db`.`shard_table` DROP INDEX idx_c2;" args:"ALTER TABLE `shard_db`.`shard_table` DROP COLUMN `c2`" sqlPattern:"~(?i)ALTER\\s+TABLE\\s+`shard_db`.`shard_table`\\s+DROP\\s+COLUMN\\s+`c2`" sharding:true  with key ~(?i)ALTER\s+TABLE\s+`shard_db`.`shard_table`\s+DROP\s+COLUMN\s+`c2` matched SQL USE `shard_db`; ALTER TABLE `shard_db`.`shard_table` DROP COLUMN `c2`;
    ```
    ```bash
    2018/12/28 16:54:36 operator.go:148: [info] [sql-operator] remove an operator uuid: eba35acd-6c5e-4bc3-b0b0-ae8bd1232351, request: name:"test" op:REPLACE args:"ALTER TABLE `shard_db`.`shard_table` DROP INDEX idx_c2;" args:"ALTER TABLE `shard_db`.`shard_table` DROP COLUMN `c2`" sqlPattern:"~(?i)ALTER\\s+TABLE\\s+`shard_db`.`shard_table`\\s+DROP\\s+COLUMN\\s+`c2`" sharding:true
    ```
7. 使用 `query-status` 确认任务 `stage` 持续为 `Running` 且不存在正阻塞同步的 DDL（`blockingDDLs`） 与待解决的 sharding group（`unresolvedGroups`）
8. 使用 `query-error` 确认不存在 DDL 执行错误
9. 使用 `show-ddl-locks` 确认不存在待解决的 DDL lock
