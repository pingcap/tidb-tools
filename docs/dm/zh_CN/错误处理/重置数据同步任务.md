重置数据同步任务
===

某些情况下需要重置数据同步任务，一般情况是上游数据库 binlog 损坏或者丢失，包括：

- 人为执行了 `RESET MASTER`
- binlog 损坏或者丢失

一般这时候 relay unit 会发生错误而退出，且无法优雅地自动恢复（后续会完善 relay 的错误处理、恢复机制）时，目前需要通过如下方式手动全量恢复数据同步。

### 操作步骤

1. 使用 `stop-task` 命令停止当前正在运行中的所有同步任务
2. 使用 ansible stop 整个 DM 集群（参见 [运维管理/DM ansible 运维手册]）
3. 手动清理掉与 binlog 被重置的 MySQL master 对应的 dm-worker 的 relay log 目录
  1. 如果采用 ansible 部署，relay log 的目录在 `<deploy_dir>/relay_log` 目录
  2. 如果是 binary 部署，relay log 目录在 `relay-dir` 参数设置的目录
4. 清理掉下游已同步的数据
5. 使用 ansible start 整个 DM 集群
6. 以新 task name 重新开始数据同步， 或设置 `remove-meta` 为 `true`，以及 `task-mode` 为 `all`
