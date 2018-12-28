query-status
===

### 索引
- [查询结果介绍](#查询结果介绍)
- [子任务状态](#子任务状态)

### 查询结果介绍

```
» query-status
{
    "result": true,     # 表示查询是否成功
    "msg": "",          # 查询失败时，失败原因的描述
    "workers": [                            # DM-worker 列表
        {
            "result": true,
            "worker": "172.17.0.2:10081",   # 该 DM-worker 的 host:port 信息
            "msg": "",
            "subTaskStatus": [              # 该 DM-worker 所有子任务信息
                {
                    "name": "test",         # 任务名称
                    "stage": "Running",     # 子任务运行状态，包含 New, Running, Paused, Stopped, Finished。不同状态的含义和状态转换关系请参考[子任务状态]
                    "unit": "Sync",         # DM 处理单元，包括 Check, Dump, Load, Sync
                    "result": null,         # 子任务出错时会在这里显示错误信息
                    "unresolvedDDLLockID": "test-`test`.`t_target`",    # sharding DDL 锁 ID，可用于异常情况下手动处理 sharding DDL lock。具体使用方法参考 <分库分表/手动处理 sharding DDL lock> 章节
                    "sync": {                   # sync 处理单元同步信息，总是显示与当前处理单元相同的组件同步信息
                        "totalEvents": "12",    # 该子任务同步的总的 binlog 事件数
                        "totalTps": "1",        # 该子任务每秒同步 binlog 事件数
                        "recentTps": "1",       # 该子任务最近1秒同步的 binlog 事件数
                        "masterBinlog": "(bin.000001, 3234)",                               # 上游数据库 binlog position
                        "masterBinlogGtid": "c0149e17-dff1-11e8-b6a8-0242ac110004:1-14",    # 上游数据库 GTID 信息
                        "syncerBinlog": "(bin.000001, 2525)",                               # sync 处理单元已经同步的 binlog position
                        "syncerBinlogGtid": "",                                             # 因为 DM 中 sync 处理单元不使用 GTID 同步，该值始终为空
                        "blockingDDLs": [       # 当前被阻塞的DDL列表，该 DM-worker 的上游表都处于 synced 状态此项才会不为空，表示待执行或跳过的 sharding DDL
                            "USE `test`; ALTER TABLE `test`.`t_target` DROP COLUMN `age`;"
                        ],
                        "unresolvedGroups": [   # 没有解决的sharding group信息
                            {
                                "target": "`test`.`t_target`",                  # 下游同步数据库表
                                "DDLs": [
                                    "USE `test`; ALTER TABLE `test`.`t_target` DROP COLUMN `age`;"
                                ],
                                "firstPos": "(bin|000001.000001, 3130)",        # sharding ddl 的起始位置
                                "synced": [                                     # sync 处理单元已经读到该 sharding DDL 的上游分表
                                    "`test`.`t2`"
                                    "`test`.`t3`"
                                    "`test`.`t1`"
                                ],
                                "unsynced": [                                   # 上游还没有执行该 sharding DDL 的表，如果还有上游表没有完成同步，blockingDDLs 的内容是空
                                ]
                            }
                        ],
                        "synced": false         # 增量同步是否追上上游，sync 处理单元后台并非实时刷新保存点，所以该同步标记为 false 时不一定存在同步延迟
                    }
                }
            ],
            "relayStatus": {    # relay log 的同步状态
                "masterBinlog": "(bin.000001, 3234)",                               # 上游数据库的 binlog position
                "masterBinlogGtid": "c0149e17-dff1-11e8-b6a8-0242ac110004:1-14",    # 上游数据库的 binlog GTID 信息
                "relaySubDir": "c0149e17-dff1-11e8-b6a8-0242ac110004.000001",       # relay log 当前的使用的 sub dir
                "relayBinlog": "(bin.000001, 3234)",                                # 已经拉取到本地的 binlog position
                "relayBinlogGtid": "c0149e17-dff1-11e8-b6a8-0242ac110004:1-14",     # 已经拉取到本地的 binlog GTID 信息
                "relayCatchUpMaster": true,     # 本地同步的 relay log 是否已经追上上游
                "stage": "Running",             # relay log 同步处理单元的运行状态
                "result": null
            }
        },
        {
            "result": true,
            "worker": "172.17.0.3:10081",
            "msg": "",
            "subTaskStatus": [
                {
                    "name": "test",
                    "stage": "Running",
                    "unit": "Load",
                    "result": null,
                    "unresolvedDDLLockID": "",
                    "load": {                   # loader 处理单元同步信息
                        "finishedBytes": "115", # 已全量导入字节数
                        "totalBytes": "452",    # 总计需要导入的字节数
                        "progress": "25.44 %"   # 全量导入进度
                    }
                }
            ],
            "relayStatus": {
                "masterBinlog": "(bin.000001, 28507)",
                "masterBinlogGtid": "c0149e17-dff1-11e8-b6a8-0242ac110004:1-96",
                "relaySubDir": "c0149e17-dff1-11e8-b6a8-0242ac110004.000001",
                "relayBinlog": "(bin.000001, 28507)",
                "relayBinlogGtid": "c0149e17-dff1-11e8-b6a8-0242ac110004:1-96",
                "relayCatchUpMaster": true,
                "stage": "Running",
                "result": null
            }
        },
        {
            "result": true,
            "worker": "172.17.0.3:10081",
            "msg": "",
            "subTaskStatus": [
                {
                    "name": "test",
                    "stage": "Running",
                    "unit": "Load",
                    "result": null,
                    "unresolvedDDLLockID": "",
                    "load": {                   # loader 处理单元同步信息
                        "finishedBytes": "115", # 已全量导入字节数
                        "totalBytes": "452",    # 总计需要导入的字节数
                        "progress": "25.44 %"   # 全量导入进度
                    }
                }
            ],
            "relayStatus": {
                "masterBinlog": "(bin.000001, 28507)",
                "masterBinlogGtid": "c0149e17-dff1-11e8-b6a8-0242ac110004:1-96",
                "relaySubDir": "c0149e17-dff1-11e8-b6a8-0242ac110004.000001",
                "relayBinlog": "(bin.000001, 28507)",
                "relayBinlogGtid": "c0149e17-dff1-11e8-b6a8-0242ac110004:1-96",
                "relayCatchUpMaster": true,
                "stage": "Running",
                "result": null
            }
        }
                {
            "result": true,
            "worker": "172.17.0.6:10081",
            "msg": "",
            "subTaskStatus": [
                {
                    "name": "test",
                    "stage": "Paused",
                    "unit": "Load",
                    "result": {                 # 出错示例
                        "isCanceled": false,
                        "errors": [
                            {
                                "Type": "ExecSQL",
                                "msg": "Error 1062: Duplicate entry '1155173304420532225' for key 'PRIMARY'\n/home/jenkins/workspace/build_dm/go/src/github.com/pingcap/tidb-enterprise-tools/loader/db.go:160: \n/home/jenkins/workspace/build_dm/go/src/github.com/pingcap/tidb-enterprise-tools/loader/db.go:105: \n/home/jenkins/workspace/build_dm/go/src/github.com/pingcap/tidb-enterprise-tools/loader/loader.go:138: file test.t1.sql"
                            }
                        ],
                        "detail": null
                    },
                    "unresolvedDDLLockID": "",
                    "load": {
                        "finishedBytes": "0",
                        "totalBytes": "156",
                        "progress": "0.00 %"
                    }
                }
            ],
            "relayStatus": {
                "masterBinlog": "(bin.000001, 1691)",
                "masterBinlogGtid": "97b5142f-e19c-11e8-808c-0242ac110005:1-9",
                "relaySubDir": "97b5142f-e19c-11e8-808c-0242ac110005.000001",
                "relayBinlog": "(bin.000001, 1691)",
                "relayBinlogGtid": "97b5142f-e19c-11e8-808c-0242ac110005:1-9",
                "relayCatchUpMaster": true,
                "stage": "Running",
                "result": null
            }
        }
    ]
}

```

### 子任务状态

- New: 初始状态，如果没有出错会转换到 Running 状态，出错转换到 Paused 状态
- Running: 正常运行状态
- Paused: 暂停状态，运行出错会进入此状态；在 Running 状态中使用 `pause-task` 也会进入此状态。可以通过 `resume-task` 命令重新开始运行任务
- Stopped: 停止状态，不可通过 `resume-task` 重新运行。在 Running 或 Paused 状态通过 `stop-task` 会进入此状态
- Finished: 运行结束状态，只有全量任务正常运行结束会进入此状态

#### 状态转换图

```
                                         error occured
                            New --------------------------------|
                             |                                  |
                             |           resume-task            |
                             |  |----------------------------|  |
                             |  |                            |  |
                             |  |                            |  |
                             v  v        error occured       |  v
  Finished <-------------- Running -----------------------> Paused
                             ^  |        or pause-task       |
                             |  |                            |
                  start task |  | stop task                  |
                             |  |                            |
                             |  v        stop task           |
                           Stopped <-------------------------|

```
