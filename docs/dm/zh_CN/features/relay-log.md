relay-log
===

### 索引
- [功能介绍](#功能介绍)
- [本地存储目录结构](#本地存储目录结构)
- [初始同步规则](#初始同步规则)
- [数据清理](#数据清理)


### 功能介绍

DM-worker 启动后会自动同步上游的 binlog 到配置的本地目录（使用 `DM-Ansible` 部署的默认同步目录为 `<deploy_dir>/relay_log`），与 [MySQL Slave Relay Log](https://dev.mysql.com/doc/refman/5.7/en/slave-logs-relaylog.html) 的使用方式类似。DM-worker 运行过程中会实时同步上游的 binlog 更新到本地文件，syncer 组件会实时读取本地的 relay-log 更新并同步更新至下游数据库。


### 本地存储目录结构

以下是一个 relay-log 本地存储的目录结构示例：

```
<deploy_dir>/relay_log/
|-- 7e427cc0-091c-11e9-9e45-72b7c59d52d7.000001
|   |-- mysql-bin.000001
|   |-- mysql-bin.000002
|   |-- mysql-bin.000003
|   |-- mysql-bin.000004
|   `-- relay.meta
|-- 842965eb-091c-11e9-9e45-9a3bff03fa39.000002
|   |-- mysql-bin.000001
|   `-- relay.meta
`-- server-uuid.index
```

- subdir 目录：DM-worker 从上游数据库同步的 binlog 会保存在同一个目录，每一个目录叫做一个 subdir，subdir 的命名格式是 <上游数据库 uuid>.<本地 subdir 序号>。当上游进行[主从切换](./master-slave-switch.md)后，DM-worker 会生成一个新的 subdir 目录，并且新目录序号递增。例如在例子中，`7e427cc0-091c-11e9-9e45-72b7c59d52d7.000001` 目录里，`7e427cc0-091c-11e9-9e45-72b7c59d52d7` 表示上游数据库 uuid，000001是本地 subdir 序号。
- server-uuid.index： 记录了当前的 subdir 列表信息
- relay.meta：在每个 subdir 内，用于保存已同步上游 binlog 的位置信息。例如

```bash
$ cat c0149e17-dff1-11e8-b6a8-0242ac110004.000001/relay.meta
binlog-name = "mysql-bin.000010"    # 当前同步的 binlog 名
binlog-pos = 63083620               # 当前同步的 binlog position
binlog-gtid = "c0149e17-dff1-11e8-b6a8-0242ac110004:1-3328" # 当前同步的 binlog GTID

# GTID 可能包含多个
$ cat 92acbd8a-c844-11e7-94a1-1866daf8accc.000001/relay.meta
binlog-name = "mysql-bin.018393"
binlog-pos = 277987307
binlog-gtid = "3ccc475b-2343-11e7-be21-6c0b84d59f30:1-14,406a3f61-690d-11e7-87c5-6c92bf46f384:1-94321383,53bfca22-690d-11e7-8a62-18ded7a37b78:1-495,686e1ab6-c47e-11e7-a42c-6c92bf46f384:1-34981190,03fc0263-28c7-11e7-a653-6c0b84d59f30:1-7041423,05474d3c-28c7-11e7-8352-203db246dd3d:1-170,10b039fc-c843-11e7-8f6a-1866daf8d810:1-308290454"
```


### 初始同步规则

DM-worker 每次启动（或 relay-log 从暂停状态恢复同步），从上游 binlog 哪个位置开始同步有以下几种情况：

* 本地有有效的 relay_log（有效指有正确的 server-uuid.index 文件、subdir 和 relay.meta 文件）：会根据 relay.meta 记录的 binlog 点继续同步
* 本地没有有效 relay_log，并且没有在 DM-worker 配置文件中指定 relay-binlog-name 或 relay-binlog-gtid：
    * 非 GTID 模式下会从上游最旧的 binlog 开始同步，依次同步上游所有 binlog 文件至最新
    * GTID 模式下，从上游初始 GTID 开始同步，如果上游 relay-log 被清理掉则会出错，使用 GTID 模式需要额外注意该点，如果出错必须指定 `relay-binlog-gtid` 提供同步起始位置
* 本地没有有效 relay_log：非 GTID 模式指定了 relay-binlog-name，从指定的 binlog 文件开始同步；GTID 模式指定了 relay-binlog-gtid，从指定 GTID 开始同步

### 数据清理

目前 relay-log 提供自动清理和手动清理两种清理方式，通过文件读写的检测机制，不会清理正在使用中或已在运行中的任务未来会使用到的 relay-log。

- 自动清理：在 DM-worker 配置文件中包括三个配置项：

    * purge-interval：单位秒，每多少秒尝试进行一次后台自动清理
    * purge-expires：单位小时，指定超过多少小时没有更新的 relay-log 会在后台自动清理中被清理掉；设置为 0 表示不按更新时间清理
    * purge-remain-space：单位GB，指定 DM-worker 机器剩余磁盘空间小于多少时，会在后台自动清理中尝试清理可以安全清理的 relay-log
    * 默认参数分别是 3600, 0, 15，代表每隔 3600 秒运行一次后台清理任务：不会按 relay-log 更新时间进行清理；如果磁盘空间小于 15GB，会尝试安全清理 relay-log

- 手动清理：使用 dmctl 提供的 purge-relay 命令，通过指定 subdir 和 binlog 文件名，清理掉在指定 binlog 之前的所有 relay-log

    比如我们当前的 relay-log 目录结构如下：

```
$ tree .
.
|-- deb76a2b-09cc-11e9-9129-5242cf3bb246.000001
|   |-- mysql-bin.000001
|   |-- mysql-bin.000002
|   |-- mysql-bin.000003
|   `-- relay.meta
|-- deb76a2b-09cc-11e9-9129-5242cf3bb246.000003
|   |-- mysql-bin.000001
|   `-- relay.meta
|-- e4e0e8ab-09cc-11e9-9220-82cc35207219.000002
|   |-- mysql-bin.000001
|   `-- relay.meta
`-- server-uuid.index

$ cat server-uuid.index
deb76a2b-09cc-11e9-9129-5242cf3bb246.000001
e4e0e8ab-09cc-11e9-9220-82cc35207219.000002
deb76a2b-09cc-11e9-9129-5242cf3bb246.000003
```

在 `dmctl` 执行以下命令的实际效果分别是

```
# 执行该命令会清空 deb76a2b-09cc-11e9-9129-5242cf3bb246.000001 目录
# e4e0e8ab-09cc-11e9-9220-82cc35207219.000002 和 deb76a2b-09cc-11e9-9129-5242cf3bb246.000003 目录保留

» purge-relay -w 10.128.16.223:10081 --filename mysql-bin.000001 --sub-dir e4e0e8ab-09cc-11e9-9220-82cc35207219.000002

# 执行该命令会清空 deb76a2b-09cc-11e9-9129-5242cf3bb246.000001、e4e0e8ab-09cc-11e9-9220-82cc35207219.000002 目录
# deb76a2b-09cc-11e9-9129-5242cf3bb246.000003 目录保留

» purge-relay -w 10.128.16.223:10081 --filename mysql-bin.000001
```
