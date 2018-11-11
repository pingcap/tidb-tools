部署目录结构
===

### dm-master

#### log 文件

由 dm-master 进程参数 `--log-file` 指定；使用 ansible 部署，则在 dm-master node 节点的 `{ansible deploy}/log/dm-master.log`

#### 配置文件

由 dm-master 进程参数 `--config` 指定; 使用 ansible 部署，则在 dm-master node 节点的 `{ansible deploy}/conf/dm-master.toml`

### dm-worker

#### log 文件

由 dm-worker 进程参数 `--log-file` 指定；使用 ansible 部署，则在 dm-worker node 节点的 `{ansible deploy}/log/dm-worker.log`

#### 配置文件

由 dm-worker 进程参数 `--config` 指定; 使用 ansible 部署，则在 dm-worker node 节点的 `{ansible deploy}/conf/dm-worker.toml`

#### relay log 目录

由 dm-worker 进程参数 `--relay-dir` 指定; 使用 ansible 部署，则在 dm-worker node 节点的 `{ansible deploy}/relay_log` 目录下

relay log 目录用于存储从上游数据库读取的 binlog 文件，并提供给所有的增量任务 sync unit 读取后同步给下游，其原理、功能与 MySQL relay log 类似，参见 <https://dev.mysql.com/doc/refman/5.7/en/slave-logs-relaylog.html>
