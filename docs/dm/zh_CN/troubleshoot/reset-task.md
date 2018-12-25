重置数据同步任务
===

某些情况下可能需要重新重置整个数据同步任务，相关情况包括：
- 上游人为执行了 `RESET MASTER`，造成 relay log 同步出错
- relay log 或上游 binlog 损坏或者丢失

一般这时候 relay unit 会发生错误而退出，且无法优雅地自动恢复，需要通过手动方式恢复数据同步。

### 操作步骤

1. 使用 `stop-task` 命令停止当前正在运行中的所有同步任务
2. 使用 ansible stop 整个 DM 集群（参见 [DM ansible 运维手册](../maintenance/dm-ansible.md)）
3. 手动清理掉与 binlog 被重置的 MySQL master 对应的 DM-worker 的 relay log 目录
  1. 如果采用 DM-Ansible 部署，relay log 的目录在 `<deploy_dir>/relay_log` 目录
  2. 如果是 binary 手动部署，relay log 目录在 `relay-dir` 参数设置的目录
4. 清理掉下游已同步的数据
5. 使用 ansible start 整个 DM 集群
6. 以新 task name 重新开始数据同步， 或设置 `remove-meta` 为 `true` 且 `task-mode` 为 `all`
