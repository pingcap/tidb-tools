dmctl 使用手册
===

使用 ansible 部署， dmctl binary 路径为 `dm-ansible/dmctl`

### 使用命令

#### help

```
$ ./dmctl --help
Usage of dmctl:
 -V	prints version and exit                 # 打印版本信息
 -encrypt string                            # 按照 DM 提供的加密方法加密数据库密码，用于 DM 的配置文件
​    	encrypt plaintext to ciphertext
 -master-addr string                        # dm-master 访问地址，dmctl 跟 dm-master 交互完成任务管理操作
​    	master API server addr
```

#### 加密数据库密码

DM 使用经过 dmctl 加密后的密码，否则会报错。对于同一个原始密码，每次加密后密文不同。

```
$ ./dmctl -encrypt 123456
VjX8cEeTX+qcvZ3bPaO4h0C80pe/1aU=
```

#### 任务管理

```
$ ./dmctl -master-addr 172.16.30.14                    # 进入与 dm-master 交互命令行模式
Welcome to dmctl
Release Version: v1.0.0-100-g2bef6f8b
Git Commit Hash: 2bef6f8beda34c0dff57377005c71589b48aa3c5
Git Branch: dm-master
UTC Build Time: 2018-11-02 10:03:18
Go Version: go version go1.11 linux/amd64

» help
DM control

Usage:
  dmctl [command]

Available Commands:
  break-ddl-lock       force to break dm-worker's DDL lock
  generate-task-config generate a task config with config file
  help                 Help about any command
  pause-relay          pause dm-worker's relay unit
  pause-task           pause a running task with name
  query-status         query task's status
  refresh-worker-tasks refresh worker -> tasks mapper
  resume-relay         resume dm-worker's relay unit
  resume-task          resume a paused task with name
  show-ddl-locks       show un-resolved DDL locks
  sql-inject           sql-inject injects (limited) sqls into syncer as binlog event
  sql-replace          sql-replace replaces sql in specific binlog_pos with other sqls, each sql must ends with semicolon;
  sql-skip             sql-skip skips specified binlog position
  start-task           start a task with config file
  stop-task            stop a task with name
  switch-relay-master  switch master server of dm-worker's relay unit
  unlock-ddl-lock      force to unlock DDL lock
  update-master-config update configure of dm-master
  update-task          update a task's config for routes, filters, column-mappings, black-white-list

Flags:
  -h, --help             help for dmctl
  -w, --worker strings   dm-worker ID

Use "dmctl [command] --help" for more information about a command.
```

### 命令

#### check-task

检查上游 MySQL 实例配置，是否满足 dm 规范要求。 详情见 [上游数据库实例检查]

#### start-task -  [创建数据同步任务] 文档

#### stop-task - [停止数据同步任务] 文档

#### pause-task - [暂停数据同步任务] 文档

#### update-task  - [更新数据同步任务] 文档

#### query-status -  [查询数据同步任务状态] 文档

#### resume-task - [重启数据同步任务状态] 文档

#### generate-task-config 

根据 dm.yaml 生成 task.yaml,  样例命令 generate-task-config  <path/dm.yaml> <path/task.yaml>

#### restore-dm-config

根据 task.yaml 生成 dm.yaml, 样例命令 restore-dm-config <path/task.yaml> <path/dm.yaml>, 目前没有实现对 task 一对一的还原，只能用于人工校验 task.yaml

#### show-ddl-locks

查询当前正在等待同步的 sharding DDL lock。详情见 [DM sharding DDL 同步与人为干预](https://internal.pingcap.net/confluence/pages/viewpage.action?pageId=14457750)

#### unlock-ddl-lock 

尝试解除当前正在等待同步的 sharding DDL lock。详情见 [DM sharding DDL 同步与人为干预](https://internal.pingcap.net/confluence/pages/viewpage.action?pageId=14457750)

#### break-ddl-lock 

强制打破 dm-worker 当前正在等待同步的 sharding DDL。详情见 [DM sharding DDL 同步与人为干预](https://internal.pingcap.net/confluence/pages/viewpage.action?pageId=14457750)

#### refresh-worker-tasks 

强制刷新 dm-master 内存中维护的 task => dm-workers 映射关系，一般不需要主动使用，只需要在确定 task => dm-workers 映射关系存在，但执行其它命令时仍提示需要刷新时使用
