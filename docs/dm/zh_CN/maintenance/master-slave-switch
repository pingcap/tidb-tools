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
3. 变更 dm-worker 配置，使用 ansible 滚动升级
4. 更新 `task.yaml` 配置，更新 [mysql-instances / config] 配置
5. `start-task` 重新启动任务
