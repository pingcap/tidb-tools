手动处理 sharding DDL lock
===

DM 的 sharding DDL lock 同步在绝大多数情况下能自动完成，但在部分异常情况发生时，需要使用 `unlock-ddl-lock` / `break-ddl-lock` 手动解除异常的 DDL lock。


### 部分 dm-worker 下线

#### lock 异常原因

在 dm-master 尝试自动解除 sharding DDL lock 之前，需要等待所有 dm-worker 的 sharding DDL 到达。如果 sharding DDL 已经在同步过程中，且有部分 dm-worker 下线并不再计划重启它们，则会由于永远无法等齐所有的 DDL 而造成 lock 无法自动同步并解除。

如果不在 sharding DDL 同步过程中时需要下线 dm-worker，更好的做法是先使用 `stop-task` 停止运行中的任务，然后下线 dm-worker，最后使用 `start-task` 及不包含已下线 dm-worker 的 **新任务配置** 重启任务。

如果在 owner 执行完 DDL 但其他 dm-worker 未跳过该 DDL 时，owner 下线了，处理方法请参阅 [部分 dm-worker 重启](#部分-dm-worker-重启-或临时不可达)。

#### 手动处理方法

1. 使用 `show-ddl-locks` 查看当前正在等待同步的 sharding DDL lock 信息
2. 使用 `unlock-ddl-lock` 命令指定要手动解除的 lock 信息
    * 如果 lock 的 owner 已经下线，可以使用 `--owner` 参数指定其他 dm-worker 作为新 owner 替代执行 DDL
3. 使用 `show-ddl-locks` 查看 lock 是否解除成功

#### 手动处理后影响

手动解除 lock 后，由于该 task 的配置信息中仍然包含了已下线的 dm-worker，当下次 sharding DDL 到达时，仍会出现 lock 无法自动完成同步的情况。

因此，在手动解除 dm-worker 后，应该使用 `stop-task` / `start-task` 及不包含已下线 dm-worker 的新任务配置重启任务。


### 部分 dm-worker 重启 （或临时不可达）

#### lock 异常原因

当前 DM 调度多个 dm-worker 执行 / 跳过 sharding DDL 及更新 checkpoint 这个解除 lock 的操作流程并不是原子的，因此可能存在 owner 执行完 DDL 后、其他 dm-worker 跳过该 DDL 前非 owner 发生了重启。此时，dm-master 上的 lock 信息已经被移除，但重启的 dm-worker 并没能跳过该 DDL 及更新 checkpoint。

重启并重新 `start-task` 后，该 dm-worker 将重新尝试同步该 sharding DDL，并由于其他 dm-worker 已完成了该 DDL 的同步，重启的 dm-worker 将无法同步并跳过该 DDL。

#### 手动处理方法

1. 使用 `query-status` 查看重启的 dm-worker 当前正 block 的 sharding DDL 信息
2. 使用 `break-ddl-lock` 命令指定要强制打破 lock 的 dm-worker 信息
    * 指定 `skip` 参数跳过该 sharding DDL
3. 使用 `query-status` 查看 lock 是否打破成功

#### 手动处理后影响

手动打破 lock 后，后续 sharding DDL 将可以自动正常同步


### dm-master 重启

#### lock 异常原因

当 dm-worker 将 sharding DDL 信息发送给 dm-master 后，dm-worker 自身将挂起并等待 dm-master 通知以决定是否执行 / 跳过该 DDL。

由于 dm-master 的状态是不持久化的，如果此时 dm-master 发生了重启，dm-worker 发送给 dm-master 的 lock 信息将丢失。

因此，dm-master 重启后，将由于缺失 lock 信息而无法调度 dm-worker 执行 / 跳过 DDL。

#### 手动处理方法

1. 使用 `show-ddl-locks` 确认 sharding DDL lock 信息已丢失
2. 使用 `query-status` 确认 dm-worker 当前正由于等待 sharding DDL lock 同步而 block
3. 使用 `pause-task` 暂停被 block 的任务
4. 使用 `resume-task` 恢复被暂停的任务，重新开始 sharding DDL lock 同步

#### 手动处理后影响

手动暂停、恢复任务后，dm-worker 会重新开始 sharding DDL lock 同步并将之前丢失的 lock 信息重新发送给 dm-master，后续 sharding DDL 将可以自动正常同步。

### sharding DDL lock 相关命令说明

参见 [分库分表数据同步](./data-synchronization.md)
