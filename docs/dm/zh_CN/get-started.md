Get Started
===

### 内容索引
- [DM 介绍](./overview.md)
- [使用限制](./restrictions.md)
- [部署 DM 集群](#部署-dm-集群)
- [启动同步任务](#启动同步任务)
- [监控与日志](#监控与日志)
- [下一步](#下一步)

### 部署 DM 集群

目前推荐使用 DM-Ansible 来部署 DM 集群，具体部署方法请参照 [DM Ansible 部署安装](./maintenance/dm-ansible.md)。

#### 注意事项

在 DM 所有配置文件中，数据库的密码需要使用 dmctl 加密后的密文（如果数据库密码为空，则不需要加密）。

了解如何使用 dmctl 加密明文密码，请参考 [dmctl 加密上游 MySQL 用户密码](./maintenance/dm-ansible.md#dmctl-加密上游-mysql-用户密码)。


### 启动同步任务

#### 1 检查集群信息

假设按上述步骤使用 DM-Ansible 部署 DM 集群后，DM 集群中相关组件配置信息如下：

| 组件 | Host | Port |
|------| ---- | ---- |
| dm_worker1 | 172.16.10.72 | 8262 |
| dm_worker2 | 172.16.10.73 | 8262 |
| dm_master | 172.16.10.71 | 8261 |

上下游数据库实例相关信息如下：

| 数据库实例 | IP | 端口 | 用户名 | 加密后密码 |
| -------- | --- | --- | --- | --- |
| 上游 MySQL-1 | 172.16.10.81 | 3306 | root | VjX8cEeTX+qcvZ3bPaO4h0C80pe/1aU= |
| 上游 MySQL-2 | 172.16.10.82 | 3306 | root | VjX8cEeTX+qcvZ3bPaO4h0C80pe/1aU= |
| 下游 TiDB | 172.16.10.83 | 4000 | root | |


使用 DM-Ansible 部署完成后，dm-master 进程配置文件 `{ansible deploy}/conf/dm-master.toml` 内容应该如下：

```toml
# Master Configuration.

[[deploy]]
source-id = "mysql-replica-01"
dm-worker = "172.16.10.72:8262"

[[deploy]]
source-id = "mysql-replica-02"
dm-worker = "172.16.10.73:8262"
```

#### 2 任务配置

假设需要将 MySQL-1 和 MySQL-2 的 `test_db` 库的 `test_table` 表都以 **全量+增量** 的模式同步到下游 TiDB 的 `test_db` 库的 `test_table` 表。


COPY 并且编辑 `{ansible deploy}/conf/task.yaml.example`，生成如下任务配置文件 `task.yaml`

```yaml
name: "test"                  # 任务名，多个同时运行的任务不能重名
task-mode: "all"              # 全量+增量 (all) 同步模式
target-database:              # 下游 TiDB 配置信息
  host: "172.16.10.83"
  port: 4000
  user: "root"
  password: ""

mysql-instances:                       # 当前数据同步任务需要的全部上游 MySQL 实例配置
-
  source-id: "mysql-replica-01"        # 上游实例或者复制组 ID，参考 inventory.ini 的 source_id 或者 dm-master.toml 的 source-id 配置
  black-white-list: "global"           # 需要同步的库名/表名黑白名单的配置项名称，用于引用全局的黑白名单配置， 全局配置见下面的 black-white-list map 配置
  mydumper-config-name: "global"       # mydumper 的配置项名称，用于引用全局的 mydumper 配置

-
  source-id: "mysql-replica-02"
  black-white-list: "global"
  mydumper-config-name: "global"

# 黑白名单全局配置，各实例通过配置项名引用
black-white-list:
  global:
    do-tables:                                    # 需要同步的上游表的白名单
    - db-name: "test_db"                          # 需要同步的表的库名
      tbl-name: "test_table"                      # 需要同步的表的表名

# mydumper 全局配置，各实例通过配置项名引用
mydumpers:
  global:
    extra-args: "-B test_db -T test_table"        # 只 dump test_db 库中的 test_table 表，可设置 MyDumper 的任何参数
```

#### 3 启动任务

进入 dmctl 目录（`/home/tidb/dm-ansible/resources/bin/`），使用以下命令启动 dmctl：
```bash
./dmctl --master-addr 172.16.10.71:8261
```

在 dmctl 命令行内，使用以下命令启动同步任务：
```bash
start-task ./task.yaml            # task.yaml 为上一步编辑的配置文件路径
```

如果启动任务成功，将返回以下信息：
```json
{
    "result": true,
    "msg": "",
    "workers": [
        {
            "result": true,
            "worker": "172.16.10.72:8262",
            "msg": ""
        },
        {
            "result": true,
            "worker": "172.16.10.73:8262",
            "msg": ""
        }
    ]
}
```

如果没有启动成功，可根据返回结果的提示进行配置变更后使用 `start-task task.yaml` 重新启动任务。

#### 4 查询任务

如果需要了解 DM 集群中是否运行有同步任务及任务状态等信息，可以在 dmctl 命令行内使用以下命令进行查询：
```bash
query-status
```

#### 5 停止任务

如果不再需要进行数据同步，可以在 dmctl 命令行内使用以下命令停止同步任务
```bash
stop-task test              # 其中的 `test` 是 `task.yaml` 配置文件中 `name` 配置项设置的任务名
```

***

### 监控与日志

#### DM 监控 dashboard

如果您是通过 [部署 DM 集群](#部署-dm-集群) 来部署 DM 集群，确保正确部署了 prometheus 与 grafana，且 grafana 的地址为 `172.16.10.71`。

在浏览器中打开 <http://172.16.10.71:3000> 进入 grafana，选择 DM 的 dashboard 即可看到 DM 相关监控项，具体各监控项的解释参见 [DM 监控与告警](./maintenance/metrics-alert.md)。

#### DM log

DM 在运行过程中，DM-worker，DM-master 及 dmctl 都会通过 log 输出相关信息。

其中，DM-worker， DM-master 的 log 文件输出目录参见 [部署目录结构](./maintenance/directory-structure.md)，dmctl 的 log 文件与其 binary 同目录。

***

### 下一步

接下来你可以按照下面的学习路线深入了解 DM，然后定制符合你业务需求的数据同步任务

  1. 阅读并且了解 [使用限制](./restrictions.md)
  2. 阅读并且了解 [配置文件](./configuration/configuration.md)
  3. 学习 [任务管理](./task-handling) 章节来管理和查看任务的运行
  4. 学习了解同步案例
     1. [简单的从库同步场景](./use-cases/one-tidb-slave.md)
     2. [分库分表合并场景](./use-cases/shard-merge.md)
