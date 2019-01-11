手动处理 sharding DDL lock
===

### 索引

- [功能介绍](#功能介绍)
- [命令介绍](#命令介绍)

### 功能介绍

DM 的 sharding DDL lock 同步在绝大多数情况下能自动完成，但在部分异常情况发生时，需要使用 `unlock-ddl-lock` / `break-ddl-lock` 手动处理异常的 DDL lock。

注意：
- 绝大多数时候，不应该使用 `unlock-ddl-lock` / `break-ddl-lock` 命令，除非完全明确当前场景下，使用这些命令可能会造成的影响，并能接受这些影响
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
    "msg": "",                                             # 查询 lock 操作失败时的原因
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

- `worker`: flag 参数，string，`--worker`，可选，可重复多次指定；不指定时对所有已经在等待该 lock 的 DM-worker （`show-ddl-locks` 返回结果中的 `synced`）发起跳过 DDL 操作请求，指定时仅对这组 DM-worker 发起跳过 DDL 操作请求
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


### 部分 DM-worker 下线

#### lock 异常原因

在 DM-master 尝试自动解除 sharding DDL lock 之前，需要等待所有 DM-worker 的 sharding DDL 到达。如果 sharding DDL 已经在同步过程中，且有部分 DM-worker 下线并不再计划重启它们，则会由于永远无法等齐所有的 DDL 而造成 lock 无法自动同步并解除。

如果不在 sharding DDL 同步过程中时需要下线 DM-worker，更好的做法是先使用 `stop-task` 停止运行中的任务，然后下线 DM-worker，最后使用 `start-task` 及不包含已下线 DM-worker 的 **新任务配置** 重启任务。

如果在 owner 执行完 DDL 但其他 DM-worker 未跳过该 DDL 时，owner 下线了，处理方法请参阅 [部分 dm-worker 重启](#部分-dm-worker-重启-或临时不可达)。

#### 手动处理方法

1. 使用 `show-ddl-locks` 查看当前正在等待同步的 sharding DDL lock 信息
2. 使用 `unlock-ddl-lock` 命令指定要手动解除的 lock 信息
    * 如果 lock 的 owner 已经下线，可以使用 `--owner` 参数指定其他 DM-worker 作为新 owner 替代执行 DDL
3. 使用 `show-ddl-locks` 查看 lock 是否解除成功

#### 手动处理后影响

手动解除 lock 后，由于该 task 的配置信息中仍然包含了已下线的 DM-worker，当下次 sharding DDL 到达时，仍会出现 lock 无法自动完成同步的情况。

因此，在手动解除 DM-worker 后，应该使用 `stop-task` / `start-task` 及不包含已下线 DM-worker 的新任务配置重启任务。

**注意**：如果 `unlock-ddl-lock` 之后，之前下线的 DM-worker 又重新上线，则
1. 这些 DM-worker 会重新同步已经被 unlock 的 DDL
2. 但其它未下线过的 DM-worker 已经同步完成了这些 DDL
3. 这些重新上线的 DM-worker 的 DDL 将尝试匹配上其它未下线过的 DM-worker 后续同步的其它 DDL
4. 不同 DM-worker 的 sharding DDL 同步匹配错误


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

