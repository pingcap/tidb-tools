手动处理 sharding DDL lock
===

### 索引

- [功能介绍](#功能介绍)
- [命令介绍](#命令介绍)
- [支持场景](#支持场景)
    - [部分 DM-worker 下线](#部分-DM-worker-下线)

### 功能介绍

DM 的 sharding DDL lock 同步在绝大多数情况下能自动完成，但在部分异常情况发生时，需要使用 `unlock-ddl-lock`/`break-ddl-lock` 手动处理异常的 DDL lock。

注意：
- 绝大多数时候，不应该使用 `unlock-ddl-lock`/`break-ddl-lock` 命令，除非完全明确当前场景下，使用这些命令可能会造成的影响，并能接受这些影响
- 在手动处理异常的 DDL lock 前，请确保已经了解 DM 的 [分库分表合并同步原理](./shard-merge.md#原理)


### 命令介绍


#### show-ddl-locks

查询当前在 DM-master 上存在的 DDL lock 信息。

##### 命令示例

```bash
show-ddl-locks [--worker=127.0.0.1:8262] [task-name]
```

##### 参数解释

- `worker`: flag 参数，string，`--worker`，可选，可重复多次指定；不指定时查询所有 DM-worker 相关的 lock 信息，指定时仅查询与这组 DM-worker 相关的 lock 信息
- `task-name`: 非 flag 参数，string，可选；不指定时查询与所有任务相关的 lock 信息，指定时仅查询特定任务相关的 lock 信息

##### 返回结果示例

```bash
» show-ddl-locks test
{
    "result": true,                                        # 查询 lock 操作本身是否成功
    "msg": "",                                             # 查询 lock 操作失败时的原因或其它描述信息（如不存在任务 lock）
    "locks": [                                             # DM-master 上存在的 lock 信息列表
        {
            "ID": "test-`shard_db`.`shard_table`",         # lock 的 ID 标识，当前由任务名与 DDL 对应的 schema/table 信息组成
            "task": "test",                                # lock 所属的任务名
            "owner": "127.0.0.1:8262",                     # lock 的 owner（第一个遇到该 DDL 的 DM-worker）
            "DDLs": [                                      # lock 对应的 DDL 列表
                "USE `shard_db`; ALTER TABLE `shard_db`.`shard_table` DROP COLUMN `c2`;"
            ],
            "synced": [                                    # 已经收到对应 MySQL 实例内所有分表 DDL 的 DM-worker 列表
                "127.0.0.1:8262"
            ],
            "unsynced": [                                  # 尚未收到对应 MySQL 实例内所有分表 DDL 的 DM-worker 列表
                "127.0.0.1:8263"
            ]
        }
    ]
}
```

---

#### unlock-ddl-lock

主动请求 DM-master unlock（解除）指定的 DDL lock，包括请求 owner 执行 DDL、请求其他非 owner 的 DM-worker 跳过 DDL、移除 DM-master 上的 lock 信息。

##### 命令示例

```bash
unlock-ddl-lock [--worker=127.0.0.1:8262] [--owner] [--force-remove] <lock-ID>
```

##### 参数解释

- `worker`: flag 参数，string，`--worker`，可选，可重复多次指定；不指定时对所有已经在等待该 lock 的 DM-worker 发起跳过 DDL 操作请求，指定时仅对这组 DM-worker 发起跳过 DDL 操作请求
- `owner`: flag 参数，string，`--owner`，可选，不指定时请求默认的 owner（`show-ddl-locks` 返回结果中的 `owner`）执行 DDL，指定时请求该 DM-worker（替代默认的 owner）执行 DDL
- `force-remove`: flag 参数，boolean，`--force-remove`，可选；不指定时仅在 owner 执行 DDL 成功时移除 lock 信息，指定时即使 owner 执行 DDL 失败也强制移除 lock 信息（此后将无法再次查询/操作该 lock）
- `lock-ID`: 非 flag 参数，string，必选；指定需要执行 unlock 操作的 DDL lock ID（`show-ddl-locks` 返回结果中的 `ID`）

##### 返回结果示例

```bash
» unlock-ddl-lock test-`shard_db`.`shard_table`
{
    "result": true,                                        # unlock lock 操作是否成功
    "msg": "",                                             # unlock lock 操作失败时的原因
    "workers": [                                           # 各 DM-worker 执行/跳过 DDL 操作结果信息列表
        {
            "result": true,                                # 该 DM-worker 执行/跳过 DDL 操作是否成功
            "worker": "127.0.0.1:8262",                    # DM-worker 地址（DM-worker ID）
            "msg": ""                                      # DM-worker 执行/跳过 DDL 失败时的原因
        }
    ]
}
```

---

#### break-ddl-lock

主动请求 DM-worker break（强制打破）当前正在等待 unlock 的 DDL lock，包括请求 DM-worker 执行/跳过 DDL、移除该 DM-worker 上的 DDL lock 信息。

##### 命令示例

```bash
break-ddl-lock <--worker=127.0.0.1:8262> [--remove-id] [--exec] [--skip] <task-name>
```

##### 参数解释

- `worker`: flag 参数，string，`--worker`，必选；指定需要执行 break 操作的 DM-worker
- `remove-id`: flag 参数，string，`--remove-id`，可选；如果指定，应为某个 DDL lock 的 ID；不指定时仅在 break 操作成功时移除对应 DDL lock 的信息，指定时强制移除该 DDL lock 的信息
- `exec`: flag 参数，boolean，`--exec`，可选；不可与 `--skip` 参数同时指定；指定时请求该 DM-worker 执行（execute）当前 lock 对应的 DDL
- `skip`: flag 参数，boolean，`--skip`，可选；不可与 `--exec` 参数同时指定；指定时请求该 DM-worker 跳过（skip）当前 lock 对应的 DDL
- `task-name`: 非 flag 参数，string，必选；指定要执行 break 操作的 lock 所在的 task 名称（各 task 上是否存在 lock 可通过 [query-status](../task-handling/query-status.md) 获得）

##### 返回结果示例

```bash
» break-ddl-lock -w 127.0.0.1:8262 --exec test
{
    "result": true,                                        # break lock 操作是否成功
    "msg": "",                                             # break lock 操作失败时的原因
    "workers": [                                           # 执行 break lock 操作的 DM-worker 列表（当前单次操作仅支持对一个 DM-worker 执行 break lock）
        {
            "result": false,                               # 该 DM-worker break lock 操作是否成功
            "worker": "127.0.0.1:8262",                    # 该 DM-worker 地址（DM-worker ID）
            "msg": ""                                      # DM-worker break lock 失败时的原因
        }
    ]
}
```


### 支持场景

目前使用 `unlock-ddl-lock`/`break-ddl-lock` 命令仅支持处理以下的 sharding DDL lock 异常情况，其他异常情况请咨询相关开发人员。


#### 部分 DM-worker 下线

##### lock 异常原因

在 DM-master 尝试自动 unlock sharding DDL lock 之前，需要等待所有 DM-worker 的 sharding DDL 全部到达（具体流程见 [分库分表合并同步原理](./shard-merge.md#原理)）。如果 sharding DDL 已经在同步过程中，且有部分 DM-worker 下线并且不再计划重启它们（按业务需求移除了这部分 DM-worker），则会由于永远无法等齐所有的 DDL 而造成 lock 无法自动 unlock。

> 如果不在 sharding DDL 同步过程中时需要下线 DM-worker，更好的做法是先使用 `stop-task` 停止运行中的任务，然后下线 DM-worker 并从任务配置文件中移除对应的配置信息，最后使用 `start-task` 及新的配置文件重新启动同步任务。

##### 手动处理示例

假设上游有 MySQL-1 和 MySQL-2 两个实例，其中 MySQL-1 中有 `shard_db_1`.`shard_table_1` 和 `shard_db_1`.`shard_table_2` 两个表，MySQL-2 中有 `shard_db_2`.`shard_table_1` 和 `shard_db_2`.`shard_table_2` 两个表。需要将这 4 个表合并后同步到下游 TiDB 的 `shard_db`.`shard_table` 表中。

初始时表结构为

```sql
mysql> SHOW CREATE TABLE shard_db_1.shard_table_1;
+---------------+------------------------------------------+
| Table         | Create Table                             |
+---------------+------------------------------------------+
| shard_table_1 | CREATE TABLE `shard_table_1` (
  `c1` int(11) NOT NULL,
  PRIMARY KEY (`c1`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 |
+---------------+------------------------------------------+
```

上游分表将执行以下 DDL 变更表结构

```sql
ALTER TABLE shard_db_*.shard_table_* ADD COLUMN c2 INT;
```

MySQL 及 DM 操作与处理流程为：

1. MySQL-1 对应的 DM-worker-1 两个分表执行了对应的 DDL 进行表结构变更
    ```sql
    ALTER TABLE shard_db_1.shard_table_1 ADD COLUMN c2 INT;
    ```
    ```sql
    ALTER TABLE shard_db_1.shard_table_2 ADD COLUMN c2 INT;
    ```
2. DM-worker-1 将对应 MySQL-1 相关的 DDL 信息发送给 DM-master，DM-master 创建相应的 DDL lock
3. 使用 `show-ddl-lock` 可查看当前的 DDL lock 信息
    ```bash
    » show-ddl-locks test
    {
        "result": true,
        "msg": "",
        "locks": [
            {
                "ID": "test-`shard_db`.`shard_table`",
                "task": "test",
                "owner": "127.0.0.1:8262",
                "DDLs": [
                    "USE `shard_db`; ALTER TABLE `shard_db`.`shard_table` ADD COLUMN `c2` int(11);"
                ],
                "synced": [
                    "127.0.0.1:8262"
                ],
                "unsynced": [
                    "127.0.0.1:8263"
                ]
            }
        ]
    }
    ```
4. 由于业务需要，DM-worker-2 对应的 MySQL-2 的数据不再需要同步到下游 TiDB，对 DM-worker-2 执行了下线处理
5. DM-master 上 ID 为 ``` test-`shard_db`.`shard_table` ``` 的 lock 无法等到 DM-worker-2 的 DDL 信息
    - `show-ddl-locks` 返回的 `unsynced` 中一直包含 DM-worker-2 的信息（`127.0.0.1:8263`） 
6. 使用 `unlock-dll-lock` 来请求 DM-master 主动 unlock 该 DDL lock
    - 如果 DDL lock 的 owner 也已经下线，可以使用 `--owner` 参数指定其他 DM-worker 作为新 owner 来执行 DDL
    - 当存在任意 DM-worker 报错时，`result` 将为 `false`，此时请仔细检查各 DM-worker 的错误是否是预期可接受的
    - 已下线的 DM-worker 会返回 `rpc error: code = Unavailable` 错误属于预期行为，可以忽略；如果其它未下线的 DM-worker 返回错误，则需要根据情况额外处理
    ```bash
    » unlock-ddl-lock test-`shard_db`.`shard_table`
    {
        "result": false,
        "msg": "github.com/pingcap/tidb-enterprise-tools/dm/master/server.go:1472: DDL lock test-`shard_db`.`shard_table` owner ExecuteDDL successfully, so DDL lock removed. but some dm-workers ExecuteDDL fail, you should to handle dm-worker directly",
        "workers": [
            {
                "result": true,
                "worker": "127.0.0.1:8262",
                "msg": ""
            },
            {
                "result": false,
                "worker": "127.0.0.1:8263",
                "msg": "rpc error: code = Unavailable desc = all SubConns are in TransientFailure, latest connection error: connection error: desc = \"transport: Error while dialing dial tcp 127.0.0.1:8263: connect: connection refused\""
            }
        ]
    }
    ```
7. 使用 `show-dd-locks` 确认 DDL lock 是否被成功 unlock
    ```bash
    » show-ddl-locks test
    {
        "result": true,
        "msg": "no DDL lock exists",
        "locks": [
        ]
    }
    ```
8. 查看下游 TiDB 中表结构是否变更成功
    ```sql
    mysql> SHOW CREATE TABLE shard_db.shard_table;
    +-------------+--------------------------------------------------+
    | Table       | Create Table                                     |
    +-------------+--------------------------------------------------+
    | shard_table | CREATE TABLE `shard_table` (
      `c1` int(11) NOT NULL,
      `c2` int(11) DEFAULT NULL,
      PRIMARY KEY (`c1`)
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin |
    +-------------+--------------------------------------------------+
    ```
9. 使用 `query-status` 确认同步任务是否正常

##### 手动处理后影响

使用 `unlock-ddl-lock` 手动执行 unlock 操作后，由于该任务的配置信息中仍然包含了已下线的 DM-worker，如果不进行处理则当下次 sharding DDL 到达时，仍会出现 lock 无法自动完成同步的情况。

因此，在手动 unlock DDL lock 后，应该再执行以下操作：

1. 使用 `stop-task` 停止运行中的任务
2. 更新任务配置文件，将已下线 DM-worker 对应的信息从配置文件中移除
3. 使用 `start-task` 及新任务配置文件重新启动任务

注意：

- 已下线的 DM-worker 在 `unlock-ddl-lock` 之后，如果对应的 DM-worker 重新上线并尝试对其中的分表进行数据同步，则会由于数据与下游表结构的不匹配而发生错误

---

### 部分 DM-worker 重启 （或临时不可达）

#### lock 异常原因

当前 DM 调度多个 DM-worker 执行 / 跳过 sharding DDL 及更新 checkpoint 这个解除 lock 的操作流程并不是原子的，因此可能存在 owner 执行完 DDL 后、其他 DM-worker 跳过该 DDL 前非 owner 发生了重启。此时，DM-master 上的 lock 信息已经被移除，但重启的 DM-worker 并没能跳过该 DDL 及更新 checkpoint。

重启并重新 `start-task` 后，该 DM-worker 将重新尝试同步该 sharding DDL，并由于其他 DM-worker 已完成了该 DDL 的同步，重启的 DM-worker 将无法同步并跳过该 DDL。

#### 手动处理方法

1. 使用 `query-status` 查看重启的 DM-worker 当前正 block 的 sharding DDL 信息
2. 使用 `break-ddl-lock` 命令指定要强制打破 lock 的 DM-worker 信息
    * 指定 `skip` 参数跳过该 sharding DDL
3. 使用 `query-status` 查看 lock 是否打破成功

#### 手动处理后影响

手动打破 lock 后，后续 sharding DDL 将可以自动正常同步。


### DM-master 重启

#### lock 异常原因

当 DM-worker 将 sharding DDL 信息发送给 DM-master 后，DM-worker 自身将挂起并等待 DM-master 通知以决定是否执行 / 跳过该 DDL。

由于 DM-master 的状态是不持久化的，如果此时 DM-master 发生了重启，DM-worker 发送给 DM-master 的 lock 信息将丢失。

因此，DM-master 重启后，将由于缺失 lock 信息而无法调度 DM-worker 执行 / 跳过 DDL。

#### 手动处理方法

1. 使用 `show-ddl-locks` 确认 sharding DDL lock 信息已丢失
2. 使用 `query-status` 确认 DM-worker 当前正由于等待 sharding DDL lock 同步而 block
3. 使用 `pause-task` 暂停被 block 的任务
4. 使用 `resume-task` 恢复被暂停的任务，重新开始 sharding DDL lock 同步

#### 手动处理后影响

手动暂停、恢复任务后，DM-worker 会重新开始 sharding DDL lock 同步并将之前丢失的 lock 信息重新发送给 DM-master，后续 sharding DDL 将可以自动正常同步。

