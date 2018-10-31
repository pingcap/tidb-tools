分库分表支持方案
===

### Background

syncer 目前采用 `ROW` 模式的 binlog，binlog 中不包含 column name 信息。ROW binlog 单机下具有 self-description 的性质，因为只有一路 binlog 更新目标数据库，可以根据下游的表结构确定新增或者修改的列值对应的 column values，进而构造出正确的 DML。

但是在 sharding 的使用情况下，因为有多路 binlog 更新一个目标数据库，并且 DDL 无法完全同步更新，所以存在 DML 与表结构不一致的情况，下面举个简单的例子

![img](./sharding-merge.png)

假设所有的表结构
```sql
(
  `id` int(11) primary key
)
```

如果存在下面的操作：
1. order_0 首先执行 `ALTER TABLE order_0 ADD COLUMN name`，并且已经同步到 order
2. 接着 order_1 执行 `INSERT INTO order_1 VALUES(12)`

那么在正常的 binlog 逻辑里面 binlog 的 row data 和 order columns 无法匹配。（注意上面，binlog 中不包含 column name 等信息, 8.0 以后支持，但是依然需要满足一些限制和假设）

### Proposed Design

#### Constraints

- 部署支持 sharding 数据同步任务的 dm-master
- 一次只能对一张表做一个 DDL
- 将要合并到同一个下游表的所有上游表都应该执行这个 DDL

#### Processing

对一个 sharding 数据迁移任务实现一个 global DDL lock

- 两层 DDL 同步
    - 第一层：dm-worker 内先进行 DDL 同步（同步相关信息由 dm-worker 保存），同步完成后才尝试进行第二层同步
    - 第二层：dm-worker 间通过 global DDL lock 协调各 dm-workers 进行同步（同步相关信息由 dm-master 与 dm-worker 共同保存）

1. dm-worker 内（原 syncer）为合并到同一个 target table 的所有上游 tables 建立一个 sharding group （第一层同步，成员为各上游 table）
2. dm-master 内为执行 target table 合并任务的所有 dm-workers 建立一个 sharding group（第二层同步, 成员为各 dm-worker）
3. 当 dm-worker 内的 sharding group 内有上游 table 遇到 DDL 时，该 dm-worker 内对这个 target table 的同步将暂停（DDL 结构变更前的 DML 同步，DDL 结构变更后的 DML 及后续 DDL 将被忽略），但对该任务其它 target table 的同步仍将继续
4. 当该 dm-worker 内的 sharding group 内的所有上游 tables 都遇到了这个 DDL 时，该 dm-worker 暂停此任务的执行（该任务其它 target table 的同步也将暂停，以方便后续 step.8 新创建的同步流追上全局同步流）
5. dm-worker 请求 dm-master 申请创建该任务上该 target table 对应的 DDL lock。如果已经存在，就注册自己信息；如果不存在，就创建 DDL lock，注册自己的信息，并且成为 DDL locker owner。DDL lock 由 _<任务, DDL>_ 相关信息一起进行标识
6. dm-master 在 dm-worker 来注册信息时根据任务的 sharding group （第二层同步）信息判断所有的 DDL 是否已经全部同步；如果已经全部同步，则通知 DDL lock owner 执行该 DDL（假设此过程中 owner 发生了 crash，则如果 owner 重启，那它会重做第一层的同步，并在第一层同步完成时触发重做第二层的同步，即会自动恢复；如果 owner 不重启，需要使用 `unlock-ddl-lock` 命令手动指定其他某个 dm-worker 代替 owner 执行该 DDL 以完成同步）
7. 判断 DDL lock owner 是否执行 DDL 成功；如果执行成功则通知正在等待该 DDL 的所有 dm-worker 开始从 DDL 之后继续同步
8. dm-worker 在从 DDL lock 恢复后开始继续同步时，新创建一条任务同步流，将原来在 step.3 时被 ignore 的 DML / DDL 重新执行一次，补全之前被跳过的数据（对于在补全过程中遇到的 DDL ，会再次开始在第一层的 sharding group 内进行同步）
9. dm-worker 将 DML 都补全完后，重新开始正常的同步

上面简述了 normal 流程，下面说一下现实中还需要解决的两个问题

- 如果 sharding DDLs 一次没有全部执行成功，或者别的原因需要回滚，设置 DDL 并不是一样的，这样子就无法正确变更状态，此时通过提供一个单独操作 DDL lock 的接口并通过 dmctl 经 dm-master 中转来调用该接口，强制进行 DDL lock 状态变更
- 如果只有部分 sharding table 需要进行 DDL（不考虑更复杂的兼容情况），可以支持这部分表形成一个 sharding group（一个独立的任务），需要在部署的拓扑里面引入这个概念
