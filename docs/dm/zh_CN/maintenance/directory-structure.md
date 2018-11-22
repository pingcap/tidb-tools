部署目录结构
===

### DM-master

#### log 文件

由 DM-master 进程参数 `--log-file` 指定；使用 ansible 部署，则在 DM-master node 节点的 `{ansible deploy}/log/dm-master.log`

#### 配置文件

由 DM-master 进程参数 `--config` 指定; 使用 ansible 部署，则在 DM-master node 节点的 `{ansible deploy}/conf/dm-master.toml`

### DM-worker

#### log 文件

由 DM-worker 进程参数 `--log-file` 指定；使用 ansible 部署，则在 DM-worker node 节点的 `{ansible deploy}/log/dm-worker.log`

#### 配置文件

由 DM-worker 进程参数 `--config` 指定; 使用 ansible 部署，则在 DM-worker node 节点的 `{ansible deploy}/conf/dm-worker.toml`

#### relay log 目录

由 DM-worker 进程参数 `--relay-dir` 指定; 使用 ansible 部署，则在 DM-worker node 节点的 `{ansible deploy}/relay_log` 目录下

relay log 目录用于存储从上游数据库读取的 binlog 文件，并提供给所有的增量任务 binlog replication/syncer 处理单元读取后同步给下游