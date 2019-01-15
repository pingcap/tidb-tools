DM 版本间升级指引
===

### 内容索引

- [指引说明](#指引说明)
- [升级到 v1.0.0-143-gcd753da](#升级到-v100-143-gcd753da)
- [升级到 v1.0.0-133-g2f9fe82](#升级到-v100-133-g2f9fe82)

### 指引说明

本文档用于描述各不完全兼容的 DM 版本间的升级指引。

假设依时间顺序从前到后存在 *V-A*, *V-B*, *V-C* 3 个互不兼容的版本，需要从 *V-A* 升级到 *V-C*。定义从 *V-A* 升级到 *V-B* 的操作为 *Upgrade-A-B*，从 *V-B* 升级到 *V-C* 的操作为 *Upgrade-B-C*。

如果 *Upgrade-A-B* 与 *Upgrade-B-C* 间存在交叠（如同一个配置项的不同变更），则推荐先执行 *Upgrade-A-B* 升级到 *V-B*，升级完成后再执行 *Upgrade-B-C* 升级到 *V-C*；或者直接参考相关的跨不兼容版本升级指引。

如果 *Upgrade-A-B* 与 *Upgrade-B-C* 间不存在交叠，则可将 *Upgrade-A-B* 与 *Upgrade-B-C* 的操作合并为 *Upgrade-A-C*，执行后直接从 *V-A* 升级到 *V-C*。

注意：

- 不特殊说明时，各版本的升级指引均为从前一个有升级指引的版本向当前版本升级
- 各升级操作示例不特殊说明时均假定已经下载了对应版本的 DM/DM-Ansible 且 DM binaries 存在于 DM-Ansible 的相应目录中
- 各升级操作示例不特殊说明时均假定升级前已停止所有同步任务，升级完成后手动重新启动所有同步任务
- 以下版本升级指引逆序展示


### 升级到 v1.0.0-143-gcd753da

#### 版本信息

```bash
Release Version: v1.0.0-143-gcd753da
Git Commit Hash: cd753da958ea9a0d5686abc9f1988b61c9d36a89
Git Branch: dm-master
UTC Build Time: 2018-12-25 06:03:11
Go Version: go version go1.11.2 linux/amd64
```

#### 主要变更

在此版本前，DM-worker 使用两个 port 向外提供不同的信息或服务

- `dm_worker_port`: 默认 10081，提供与 DM-master 通信的 RPC 服务
- `dm_worker_status_port`: 默认 10082，提供 metrics/status 等信息

从此版本开始，DM-worker 使用同一个端口（默认 8262）同时提供上述两类信息或服务。

#### 升级操作示例

1. 变更 `inventory.ini` 配置信息
    - 移除所有 `dm_worker_status_port` 配置项，根据需要变更 `dm_worker_port` 配置项
    
    如将
    ```bash
    dm_worker1_1 ansible_host=172.16.10.72 server_id=101 deploy_dir=/data1/dm_worker dm_worker_port=10081 dm_worker_status_port=10082 mysql_host=172.16.10.81 mysql_user=root mysql_password='VjX8cEeTX+qcvZ3bPaO4h0C80pe/1aU=' mysql_port=3306
    ```
    变更为
    ```bash
    dm_worker1_1 ansible_host=172.16.10.72 server_id=101 deploy_dir=/data1/dm_worker dm_worker_port=8262 mysql_host=172.16.10.81 mysql_user=root mysql_password='VjX8cEeTX+qcvZ3bPaO4h0C80pe/1aU=' mysql_port=3306
    ```
2. 使用 DM-Ansible 滚动升级 DM、Prometheus 与 Grafana


---

### 升级到 v1.0.0-133-g2f9fe82

#### 版本信息

```bash
Release Version: v1.0.0-133-g2f9fe82
Git Commit Hash: 2f9fe827d668add6493b2a3da107e0a01b94c6d1
Git Branch: dm-master
UTC Build Time: 2018-12-19 04:58:46
Go Version: go version go1.11.2 linux/amd64
```

#### 主要变更

在此版本前，任务配置文件（`task.yaml`）中的 `mysql-instances` 包含以下信息

- `config`: 上游 MySQL 的地址、用户名、密码等
- `instance-id`: 标识一个上游 MySQL

从此版本开始，上述两项配置信息被移除，并增加了如下配置信息

- `source_id`: 存在于 `inventory.ini` 中，用于标识一个上游 MySQL 实例或一个主从复制组
- `source-id`: 存在于任务配置文件的 `mysql-instances` 中，其取值与 `inventory.ini` 中的 `source_id` 对应

#### 升级操作示例

1. 变更 `inventory.ini` 配置信息
    - 为所有 DM-worker 实例设置对应的 `source_id`
    
    如将
    ```bash
    dm-worker1 ansible_host=172.16.10.72 server_id=101 mysql_host=172.16.10.72 mysql_user=root mysql_password='VjX8cEeTX+qcvZ3bPaO4h0C80pe/1aU=' mysql_port=3306
    ```
    变更为
    ```bash
    dm-worker1 ansible_host=172.16.10.72 source_id="mysql-replica-01" server_id=101 mysql_host=172.16.10.72 mysql_user=root mysql_password='VjX8cEeTX+qcvZ3bPaO4h0C80pe/1aU=' mysql_port=3306
    ```
2. 使用 DM-Ansible 滚动升级 DM
3. 变更任务配置文件（`task.yaml`）
    - 移除其中的 `config` 与 `instance-id` 配置项，增加 `source-id` 配置项（与 `inventory.ini` 中的 `source_id` 对应）
    如将
    ```yaml
    config:
          host: "192.168.199.118"	
          port: 4306	
          user: "root"	
          password: "1234"	
        instance-id: "instance118-4306" # unique in all instances, used as id when save checkpoints, configs, etc.
    ```
    变更为
    ```yaml
    source-id: "instance118-4306" # unique in all instances, used as id when save checkpoints, configs, etc.
    ```
4. 使用变更后的任务配置重新启动任务
