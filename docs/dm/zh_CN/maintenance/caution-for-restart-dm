重启 DM 组件注意事项
===

### dm-worker

#### 全量导入过程中

对于全量导入时的 SQL 文件，DM 使用了下游数据库来记录断点信息

当 dm-worker 重启时，使用 `start-task` 命令并根据断点信息即可自动恢复同步

#### 增量导入过程中

对于增量导入时的 binlog，DM 使用了下游数据库来记录断点信息，并在任务开始（或恢复）执行的前 5 分钟启用 safe mode（数据同步 SQL 幂等）

##### 未启用 sharding DDL 同步

如果 dm-worker 上运行的任务没有启用 sharding DDL 同步功能，当 dm-worker 重启时，使用 `start-task` 命令并根据断点信息即可自动恢复同步

##### 启用 sharding DDL 同步

DM 在进行 sharding DDL 同步时，如果 dm-worker 成功执行（或跳过）了 sharding DDL，则该 dm-worker 内与该 sharding DDL 相关的所有 table 的断点将被更新到 DDL 对应的 binlog 之后

如果 dm-worker 重启发生在 sharding DDL 同步前（或同步完成后），则使用 `start-task` 命令并根据断点信息即可自动恢复同步

如果 dm-worker 重启发生在 sharding DDL 同步过程中，则可能出现 owner 执行了 DDL 并成功变更了下游表结构，而其他 dm-worker 被重启而未能跳过 DDL 并更新断点的情况。此时这些未能跳过的 DDL 将再次尝试同步，但由于其他未重启的 dm-worker 已经执行到该 DDL 之后，因此重启的 dm-worker 将 block 在 DDL 对应的 binlog 处。此时需要根据 [分库分表/手动处理 sharding DDL lock] 中的说明手动进行处理

#### 总结

尽量避免在 sharding DDL 同步过程中重启 dm-worker

### dm-master

dm-master 中维护的信息主要包括以下两类，且在重启时不会进行持久化

- task 与 dm-worker 的对应关系
- sharding DDL lock 相关信息

当 dm-master 重启时，会自动从各 dm-worker 请求 task 信息并重建 task 与 dm-worker 的对应关系

但 dm-master 重启时，dm-worker 不会重新发送 sharding DDL 信息，因此会造成 lock 信息丢失而无法自动完成 sharding DDL lock 同步。此时需要根据 [分库分表/手动处理 sharding DDL lock] 中的说明手动进行处理

#### 总结

尽量避免在 sharding DDL 同步过程中重启 dm-master

### dmctl

dmctl 完全无状态，可随时重启
