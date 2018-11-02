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
