skip 或 replace 异常 SQL
===

### 索引

- [功能介绍](#功能介绍)
- [命令介绍](#命令介绍)
- [使用示例](#使用示例)
    - [出错后被动 skip](#出错后被动-skip)
    - [出错前主动 replace](#出错前主动-replace)
    - [合库合表场景下出错前主动 replace](#合库合表场景下出错前主动-replace)


### 功能介绍

目前 TiDB 并不兼容所有的 MySQL 语法（TiDB 支持的 DDL 语法见 <https://pingcap.com/docs-cn/sql/ddl/>），当使用 DM 从 MySQL 同步数据到 TiDB 时，可能会由于 TiDB 不支持对应 SQL 语句报错而造成同步中断。

当由于 TiDB 不支持的 SQL 导致同步出错时，可以使用 dmctl 来手动选择跳过该 SQL 对应的 binlog event 或使用其它指定的 SQL 来替代对应的 binlog event 向下游执行以恢复同步任务。

当提前预知会有 TiDB 不支持的 SQL 将要被同步时，可以使用 dmctl 来手动预定跳过/替代操作，当同步该 SQL 对应的 binlog event 时自动执行预定的操作，避免同步过程被中断。

注意：
- skip 或 replace 只适合用于跳过/替代执行下游 TiDB 不支持的 SQL，其它同步错误请不要使用此方式进行处理
- 单次 skip 或 replace 操作都是针对单个 binlog event 的
- `--sharding` 操作都是针对 sharding DDL lock 的 owner 的，且此时只能使用 `--sql-pattern` 来进行匹配


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

- `worker`: flag 参数，string，`--worker`，可选；不指定时查询所有 DM-worker 上的错误，指定时仅查询特定的一组 DM-worker 上的错误
- `task-name`: 非 flag 参数，string，可选；不指定时查询所有任务内的错误，指定时仅查询特定任务内的错误


#### sql-skip

预定在 binlog event 的 position 或 SQL 与指定的 `binlog-pos` 或 `sql-pattern` 匹配时执行该跳过（skip）操作。

##### 命令样例

```bash
sql-skip <--worker=127.0.0.1:8262> [--binlog-pos=mysql-bin|000001.000003:3270] [--sql-pattern=~(?i)ALTER\s+TABLE\s+`db1`.`tbl1`\s+ADD\s+COLUMN\s+col1\s+INT] [--sharding] <task-name>
```

##### 参数解释

- `worker`: flag 参数，string，`--worker`，未指定 `--sharding` 时必选，指定 `--sharding` 时禁止使用；在指定时表示预定操作将生效的 DM-worker
- `binlog-pos`: flag 参数，string，`--binlog-pos`，与 `--sql-pattern` 必须指定其中一个、且只能指定其中一个；在指定时表示操作将在 binlog event 的 position 匹配时生效，格式为 `binlog-filename:binlog-pos`（比如：`mysql-bin|000001.000003:3270`，在同步已经发生错误时，可通过 `query-error` 返回的 `failedBinlogPosition` 获得）
- `sql-pattern`: flag 参数，string，`--sql-pattern`，与 `--binlog-pos` 必须指定其中一个、且只能指定其中一个；在指定时表示操作将在 binlog event 对应的 DDL 经过可选的 router-rule 转换后匹配时生效，格式为以 `~` 为前缀的正则表达式或完整的待匹配文本（比如：``` ~(?i)ALTER\s+TABLE\s+`db1`.`tbl1`\s+ADD\s+COLUMN\s+col1\s+INT ```，字符串内部不支持空格，如需要使用空格，应使用正则表达式的 `\s+` 替代）
    - 注意：
        - 使用正则表达式时，以 `~` 为前缀，语法参考 <https://golang.org/pkg/regexp/syntax/#hdr-Syntax>
        - 正则中的 schema 和 table 名必须是经过可选的 router-rule 转换后的名字，即对应下游的 target schema / table 名。如上游为 ``` `shard_db_1`.`shard_tbl_1` ```，下游为 ``` `shard_db`.`shard_tbl` ```，则应该尝试匹配 ``` `shard_db`.`shard_tbl` ```
        - 正则中的 schema 名、table 名及 column 名需要使用 ``` ` ``` 标记，如：``` `db1`.`tbl1` ```
        - 暂时不支持正则表达式中包含原始的空格，需要使用 `\s` 或 `\s+` 替代空格
- `sharding`: flag 参数，boolean，`--sharding`，未指定 `--worker` 时必选，指定 `--worker` 时禁止使用；在指定时表示预定的操作将在 sharding DDL 同步过程中的 DDL lock owner 内生效
- `task-name`: 非 flag 参数，string，必选；表示预定的操作将生效的 task


#### sql-replace

预定在 binlog event 的 position 或 SQL 与指定的 `binlog-pos` 或 `sql-pattern` 匹配时执行该替代（replace）操作。

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
2. 使用 `sql-skip` 命令预定一个 binlog event 跳过（skip）操作（该操作将在 `resume-task` 后执行到该 binlog event 时生效）
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

假设使用 DM 进行同步，则当同步到该 DDL 对应的 binlog event 时，会由于 TiDB 不支持该 DDL 而导致 DM 同步任务中断且报如下错误：

```bash
exec sqls[[USE `db2`; ALTER TABLE `db2`.`tbl2` DROP COLUMN `c2`;]] failed, err:Error 1105: can't drop column c2 with index covered now
```

**但如果我们在上游实际执行该 DDL 前，已经知道该 DDL 不被 TiDB 所支持。** 则我们可以使用 `sql-replace` 为此 DDL 提前预定一个跳过（skip）/替代执行（replace）操作。

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
3. 使用 `sql-replace` 命令预定一个 binlog event 替代执行（replace）操作（该操作将在同步到该 binlog event 时生效）
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
5. 观察下游是否表结构变更成功，对应 DM-worker 节点中也可以看到类似如下 log：
    ```bash
    2018/12/28 15:33:45 operator.go:173: [info] [sql-operator] sql-pattern ~(?i)ALTER\s+TABLE\s+`db2`.`tbl2`\s+DROP\s+COLUMN\s+`c2` matched SQL USE `db2`; ALTER TABLE `db2`.`tbl2` DROP COLUMN `c2`;, applying operator uuid: c699a18a-8e75-47eb-8e7e-0e5abde2053c, pattern: ~(?i)ALTER\s+TABLE\s+`db2`.`tbl2`\s+DROP\s+COLUMN\s+`c2`, op: REPLACE, args: ALTER TABLE `db2`.`tbl2` DROP INDEX idx_c2; ALTER TABLE `db2`.`tbl2` DROP COLUMN `c2`
    ```
6. 使用 `query-status` 确认任务 `stage` 持续为 `Running`
7. 使用 `query-error` 确认不存在 DDL 执行错误


#### 合库合表场景下出错前主动 replace

TODO
