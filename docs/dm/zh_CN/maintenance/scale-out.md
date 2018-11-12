扩充/缩减 DM 集群
===

**注意：在变更节点前，请确保所有 sharding DDL 已同步完成**

### 增加 dm-worker 实例

#### 已有任务不需要扩展到新增加的 dm-worker 运行

1. 参考 [DM Ansible 运维手册](./dm-ansible.md) 中的 增加 dm-worker 实例

#### 已有任务需要扩展到新增加的 dm-worker 运行

##### 如果原任务启用了 sharding DDL 同步

1. 通过 dmctl 使用 `stop-task` 命令停止运行中的任务
2. 增加 dm-worker 实例并启动，重启 `dm-master`
3. 修改 `task.yaml` 任务配置，为新 dm-worker 配置合适的 `mysql-instances` 等信息
4. 通过 dmctl 使用 `start-task` 命令启动修改后的任务

##### 如果原任务未启用 sharding DDL 同步

1. 增加 `dm-worker` 实例并启动，重启 dm-master
2. 修改 `task.yaml` 任务配置，为新 dm-worker 配置合适的 `mysql-instances` 等信息
3. 通过 dmctl 使用 `start-task` 并指定 `--worker` 参数单独启动新增实例上的任务

### 删除 dm-worker 实例

#### 删除当前未运行任务的 dm-worker

1. 执行 `ansible-playbook stop.yml --tags=dm-worker -l dm_worker_label` 停止 dm-worker
2. （可选）手动删除 dm-worker
3. 执行 `ansible-playbook rolling_update.yml --tags=dm-master` 重启 dm-master

#### 删除当前已运行任务的 dm-worker

#### 如果原任务启用了 sharding DDL 同步

1. 通过 dmctl 使用 `stop-task` 命令停止运行中的任务
2. 删除 dm-worker 实例，重启 dm-master
3. 修改 `task.yaml` 任务配置，删除相关 `mysql-instances` 等信息
4. 通过 dmctl 使用 `start-task` 命令启动修改后的任务

#### 如果原任务未启动 sharding DDL 同步

1. 删除 dm-worker 实例，重启 dm-master
2. 修改 `task.yaml` 任务配置，删除相关 `mysql-instances` 等信息以方便后续任务操作

### 替换 dm-worker 实例

将替换拆分成 **删除 dm-worker 实例** 与 **增加 dm-worker 实例** 分开操作

### 替换 dm-master 实例

1. 执行 `ansible-playbook stop.yml --tags=dm-master` 停止原 dm-master 实例
2. 编辑 `inventory.ini` 修改 dm-master 配置
3. 执行 `ansible-playbook deploy.yml --tags=dm-master` 部署新 dm-master 实例
4. 执行 `ansible-playbook start.yml --tags=dm-master` 启动新 dm-master 实例
