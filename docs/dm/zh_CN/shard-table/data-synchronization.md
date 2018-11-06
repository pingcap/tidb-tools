分库分表数据同步
===

### sharding DDL 使用限制

- 在一个 sharding group 内的所有表都必须以相同的顺序执行相同的 DDL
    - 暂时不支持 sharding DDL 乱序交叉同步
    - 只要有任意一个表的 DDL 未执行，同步过程都会被挂起
- 一个 sharding group 必须等待一个 DDL 完全执行完毕后，才能执行下一个 DDL
- sharding group 数据同步任务不支持 `DROP DATABASE / TABLE`
- sharding group 支持 `RENAME TABLE`，但是有下面的限制
    - 只支持 `RENAME TABLE` 到一个不存在的表
    - 一个 `RENAME TABLE` 语句只能有一个 `RENAME` 操作 （online DDL 有别的方案支持）
- 各表任务开始时的同步位置点应当在需要同步的所有 sharding DDL 之前（或之后）
    - 如果开始同步位置点时有部分 sharding DDL 已经执行，则该 sharding DDL 将永远不能同步匹配成功
- 如果需要变更 `router-rules`，需要先等所有 sharding DDL 同步完成
    - 当 sharding DDL 在同步过程中时，使用 dmctl 尝试变更 `router-rules` 会报错
- 如果需要 `CREATE` 新表加入到已有的 sharding group 中，需要保持和最新更改的表结构一致
    - 比如原 table_a, table_b 初始时有 (a, b) 两列，sharding DDL 后有 (a, b, c) 三列，则同步完成后新 `CREATE` 的表应当有 (a, b, c) 三列
- sharding DDL lock 等待同步时，如果重启了 dm-master，会由于 lock 信息丢失而造成同步过程 block

### 同步过程人为干预流程

1. 使用 dmctl 执行 `show-ddl-locks` 命令查看当前正在等待同步的 sharding DDL 相关信息
2. 如果发现有部分 dm-workers 由于某些异常一直未能同步，可尝试使用 `unlock-ddl-lock` 命令手动尝试进行 DDL 同步
3. 如果使用 `unlock-ddl-lock` 手动同步不能成功或不能满足当前的需求，可使用 `query-status` 查看 dm-workers 当前的 sharding DDL 执行状态
4. 如果通过 `query-status` 发现 `unresolvedDDLLockID` 有当前正在等待同步的 DDL，可尝试使用 `break-ddl-lock` 强制打破当前正在等待同步的 DDL 并继续同步

### 命令参数说明

#### show-ddl-locks

- `task-name`: 非 flag 参数，string，可选；不指定时不限定查询的 task，指定时仅查询该 task
- `worker`: flag 参数，string array，`--worker`，可选，可重复多次指定；如果指定，仅查询与这些 dm-workers 相关的 DDL lock 信息

#### unlock-ddl-lock

- `lock-ID`: 非 flag 参数，string，必须指定；指定要尝试 unlock 的 DDL lock 的 ID（可通过 `show-ddl-locks` 获得）
- `owner`: flag 参数，string，`--owner`，可选；如果指定时，应该对应某一个 dm-worker，该 dm-worker 将替代默认的 owner 来执行这个 lock 对应的 DDL
- `force-remove`: flag 参数，boolean，`--force-remove`，可选；如果指定时，即使 owner 执行 DDL 失败，lock 信息也将被移除，将失去后续重试或进行其它操作的机会
- `worker`: flag 参数，string array，`--worker`，可选，可重复多次指定；如果不指定，将尝试对所有已经在等待该 lock 的 dm-worker 执行 / 跳过 DDL 操作，如果指定，将仅对指定的这些 dm-workers 执行 / 跳过 DDL

#### break-ddl-lock

- `task-name`: 非 flag 参数，string，必须指定；指定要 break 的 lock 所在的 task 名称
- `worker`: flag 参数，string，`--worker`，必须指定且只能指定一个；指定要执行 break 操作的 dm-worker
- `remove-id`: flag 参数，string，`--remove-id`，可选；如果指定，应该为某个 DDL lock 的 ID，指定后 dm-worker 上记录的关于这个 DDL lock 的信息将被移除
- `exec`: flag 参数，boolean，`--exec`，可选；如果指定，将尝试在该 dm-worker 上执行这个 lock 对应的 DDL，不可与 `skip` 参数同时指定
- `skip`: flag 参数，boolean，`--skip`，可选；如果指定，将尝试在该 dm-worker 上跳过这个 lock 对应的 DDL，不可与 `exec` 参数同时指定

### 人为 unlock / break DDL 同步的适用场景

**注意**：绝大多数时候，不应该使用 `unlock-ddl-lock` / `break-ddl-lock` 命令，除非完全明确当前场景下，使用这些命令可能会造成的影响，并能接受这些影响

#### unlock-ddl-lock

##### 在同步过程中有部分 dm-worker 下线且不计划再重启同步

- 如果原 owner 下线了，可使用 `--owner` 指定其它 dm-worker 为新的 owner 去替代原 owner 执行 DDL
- **注意**：如果 `unlock-ddl-lock` 之后，之前下线的 dm-worker 又重新上线
    1. 这些 dm-worker 会重新同步已经被 unlock 的 DDL
    2. 但其它未下线过的 dm-worker 已经同步完了这些 DDL
    3. 这些重新上线的 dm-worker 的 DDL 将匹配上其它未下线过的 dm-worker 后续同步的其它 DDL
    4. 不同 dm-worker 的 sharding DDL 同步匹配错误

##### 以错误的不同的顺序在不同库、表上执行了不同的 DDL

- 不同的 dm-worker 将等待不同的 lock 同步完成，比如 dm-worker1 等待 db1-table1，但 dm-worker2 等待 db2-table2
    1. 可先通过 `--worker` 参数指定 dm-worker2 完成 db2-table2 的同步
    2. db2-table2 同步完成后，dm-worker2 的 db1-table1 DDL 到达并正常自动同步成功
    3. dm-worker1 的 db2-table2 的 DDL 到达，但不能再与 dm-worker2 的 db2-table2 匹配
    4. 需要再次通过指定 `--worker` 参数让 dm-worker1 完成 db2-table2 的同步
- **注意**：如果有较多数量的表、或有超过 2 条的 DDL 发生了乱序，很难通过这种方式处理成功

#### break-ddl-lock

##### dm-worker / dm-master 程序异常导致 DDL lock 信息记录出错

- 通过 `--worker` 指定要强制 break lock 的 dm-worker
- 该 dm-worker break lock 后，该 DDL 对应的 lock 必定无法再自动同步成功
- 需要再配合使用 `unlock-ddl-lock`，且可能会发生 DDL 匹配错误（匹配到同表的其它 DDL）
