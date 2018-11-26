使用 dmctl 执行主-从切换
===

### 上游 VIP 后 master-slave 切换

1. 使用 `query-status` 确认 relay 已经追上切换前的 master（`relayCatchUpMaster`）
2. 使用 `pause-relay` 暂停 relay
3. 使用 `pause-task` 暂停所有运行中的任务
4. 上游 VIP 后的 master-slave 执行切换
5. 使用 `switch-relay-master` 通知 relay 执行主-从切换
6. 使用 `resume-relay` 恢复 relay 从新 master 读取 binlog
7. 使用 `resume-task` 恢复之前的任务

### 变更 IP 切换 master-slave

1. 使用 `query-status` 确认 relay 已经追上切换前的 master（`relayCatchUpMaster`）
2. `stop-task` 停止运行中的任务
3. 上游 master-slave 执行切换
4. 如果 DM-master 配置的 mysql-instance 为 `ip:port` 形式，更新 `dm-master.toml` 的 `deploy / mysql-instance` 配置，然后使用 `update-master-config` 更新 DM-master
5. 变更 DM-worker 配置，使用 ansible 滚动升级 DM-worker
6. 更新 `task.yaml` 配置，更新 `mysql-instances / config` 配置
7. `start-task` 重新启动任务
