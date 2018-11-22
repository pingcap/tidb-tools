手动处理 sharding DDL lock
===

DM 的 sharding DDL lock 同步在绝大多数情况下能自动完成，但在部分异常情况发生时，需要使用 `unlock-ddl-lock` / `break-ddl-lock` 手动解除异常的 DDL lock。

**注意**：绝大多数时候，不应该使用 `unlock-ddl-lock` / `break-ddl-lock` 命令，除非完全明确当前场景下，使用这些命令可能会造成的影响，并能接受这些影响。


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


### sharding DDL lock 相关命令说明

#### show-ddl-locks

- `task-name`: 非 flag 参数，string，可选；不指定时不限定查询的 task，指定时仅查询该 task
- `worker`: flag 参数，string array，`--worker`，可选，可重复多次指定；如果指定，仅查询与这些 DM-workers 相关的 DDL lock 信息

#### unlock-ddl-lock

- `lock-ID`: 非 flag 参数，string，必须指定；指定要尝试 unlock 的 DDL lock 的 ID（可通过 `show-ddl-locks` 获得）
- `owner`: flag 参数，string，`--owner`，可选；如果指定时，应该对应某一个 DM-worker，该 DM-worker 将替代默认的 owner 来执行这个 lock 对应的 DDL
- `force-remove`: flag 参数，boolean，`--force-remove`，可选；如果指定时，即使 owner 执行 DDL 失败，lock 信息也将被移除，将失去后续重试或进行其它操作的机会
- `worker`: flag 参数，string array，`--worker`，可选，可重复多次指定；如果不指定，将尝试对所有已经在等待该 lock 的 DM-worker 执行 / 跳过 DDL 操作，如果指定，将仅对指定的这些 DM-workers 执行 / 跳过 DDL

#### break-ddl-lock

- `task-name`: 非 flag 参数，string，必须指定；指定要 break 的 lock 所在的 task 名称
- `worker`: flag 参数，string，`--worker`，必须指定且只能指定一个；指定要执行 break 操作的 DM-worker
- `remove-id`: flag 参数，string，`--remove-id`，可选；如果指定，应该为某个 DDL lock 的 ID，指定后 DM-worker 上记录的关于这个 DDL lock 的信息将被移除
- `exec`: flag 参数，boolean，`--exec`，可选；如果指定，将尝试在该 DM-worker 上执行这个 lock 对应的 DDL，不可与 `skip` 参数同时指定
- `skip`: flag 参数，boolean，`--skip`，可选；如果指定，将尝试在该 DM-worker 上跳过这个 lock 对应的 DDL，不可与 `exec` 参数同时指定
