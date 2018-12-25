用户使用手册
===

### 快速开始

1. 阅读并且了解 [使用限制](./restrictions.md) 文档
2. 阅读并且了解 [配置文件](#配置文件) 章节
3. 阅读并且了解 [同步功能介绍](#同步功能介绍) 章节
4. 根据 [DM Ansible 运维手册](./maintenance/dm-ansible.md) 文档部署和管理 DM 集群
    1. 部署 DM 集群组件（包括 DM-master、DM-worker、dmctl）
    2. 部署监控组件（包括 prometheus、grafana、alertmanager）
    3. 启动集群
    4. 关闭集群
    5. 升级组件版本
5. 根据下文的 [Task 配置生成](#task-配置生成) 生成数据同步任务配置 `task.yaml`
6. 学习 [任务管理](#任务管理) 章节来管理和查看任务的运行


### 配置文件

#### DM 进程配置文件介绍

1. `inventory.ini` - DM-Ansible 部署配置文件，需要用户根据自己的机器拓扑进行编辑。 详情见 [DM Ansible 运维手册](./maintenance/dm-ansible.md)
2. `dm-master.toml` - DM-master 进程运行的配置文件，包含 DM 集群的拓扑信息， MySQL instance 和 DM-worker 的对应关系（必须是一对一的关系）。使用 DM-Ansible 部署时会自动生成该文件
3. `dm-worker.toml` - DM-worker 进程运行的配置文件，包含访问上游 MySQL instance 的配置信息。使用 DM-Ansible 部署时会自动生成该文件

#### task-配置生成

##### 配置文件

如果使用 DM-Ansible 部署 DM，可以在 `<path-to-dm-ansible>/conf` 找到下面任务配置文件样例

- `task.yaml.exmaple` -  数据同步任务的标准配置文件（一个特定的任务对应一个 `task.yaml`）。配置项解释见 [Task 配置文件介绍](./configuration/configuration.md)

##### 同步任务生成

- 直接基于 `task.yaml.example` 样例文件
    1. copy `task.yaml.example` 为 `your_task.yaml`
    2. 参照 [Task 配置文件介绍](./configuration/configuration.md)， 修改 `your_task.yaml` 的配置项

修改完 `your_task.yaml` 后，通过 dmctl 继续创建您的数据同步任务（参考 [任务管理](#任务管理) 章节）

##### 关键概念

| 概念         | 解释                                                         | 配置文件                                                     |
| ------------ | ------------------------------------------------------------ | ------------------------------------------------------------ |
| instance-id  | 唯一确定一个 MySQL / MariaDB 实例（使用 DM-Ansible 部署时会用 host:port 来组装成该 ID） | `dm-master.toml` 的 `mysql-instance`;<br> `task.yaml` 的 `instance-id` |
| DM-worker ID | 唯一确定一个 DM-worker （取值于 `dm-worker.toml` 的 `worker-addr` 参数） | `dm-worker.toml` 的 `worker-addr`;<br> dmctl 命令行的 `-worker` / `-w` flag  |

mysql-instance 和 DM-worker 必须一一对应


### 任务管理

#### dmctl 管理任务

使用 dmctl 可以完成数据同步任务的日常管理功能，详细解释见 [dmctl 使用手册](./task-handling/dmctl-manual.md)


### 运维管理

- DM 监控项介绍 - [DM 监控与告警](./maintenance/metrics-alert.md)

- 扩充/缩减 DM 集群 - [扩充/缩减 DM 集群](./maintenance/scale-out.md)
