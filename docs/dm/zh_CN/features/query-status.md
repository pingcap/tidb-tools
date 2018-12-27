query-status
===

### 索引
- [查询结果介绍](#查询结果介绍)
- [子任务状态](#子任务状态)

### 查询结果介绍

```
» query-status
{
    "result": true,
    "msg": "",
    "workers": [                            # DM-worker 列表
        {
            "result": true,
            "worker": "172.17.0.2:10081",   # 该 DM-worker 的 host:port 信息
            "msg": "",
            "subTaskStatus": [              # 该 DM-worker 所有子任务信息
                {
                    "name": "test",         # 任务名称
                    "stage": "Running",     # 子任务运行状态，包含 New, Running, Paused, Stopped, Finished。不同状态的含义和状态转换关系请参考 #子任务状态
                    "unit": "Sync",         # DM 工作组件，包括 Check, Dump, Load, Sync
                    "result": null,
                    "unresolvedDDLLockID": "",  # sharding DDL 锁ID
                    "sync": {                   # sync 组件同步信息，总是显示与当前工作组件相同的组件同步信息
                        "totalEvents": "12",    # 该子任务同步的总的 binlog 事件数
                        "totalTps": "1",        # 该子任务每秒同步 binlog 事件
                        "recentTps": "1",       # 目前同 totalTps
                        "masterBinlog": "(bin.000001, 3234)",                               # 上游数据库 binlog position
                        "masterBinlogGtid": "c0149e17-dff1-11e8-b6a8-0242ac110004:1-14",    # 上游数据库 GTID 信息
                        "syncerBinlog": "(bin.000001, 2525)",                               # syncer 组件已经同步的 binlog position
                        "syncerBinlogGtid": "",                                             # syncer 组件已经同步的 GTID 信息
                        "blockingDDLs": [       # 当前被阻塞的DDL列表
                        ],
                        "unresolvedGroups": [   # 没有解决的sharding group信息
                            {
                                "target": "`test`.`t_target`",                  # 下游同步数据库表
                                "DDLs": [
                                    "USE `test`; ALTER TABLE `test`.`t_target` DROP COLUMN `age`;"
                                ],
                                "firstPos": "(bin|000001.000001, 3130)",        # 最先阻塞的 binlog position
                                "synced": [                                     # 上游已经执行完该 sharding DDL 的表
                                    "`test`.`t2`"
                                ],
                                "unsynced": [                                   # 上游还没有执行该 sharding DDL 的表
                                    "`test`.`t1`"
                                ]
                            }
                        ],
                        "synced": false         # 增量同步是否追上上游
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
                "stage": "Running",             # relay log 同步组件的运行状态
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
                    "load": {                   # loader 组件同步信息
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
    ]
}

```

### 子任务状态

- New: 初始状态，如果没有出错会转换到 Running 状态，出错转换到 Paused 状态
- Running: 正常运行状态
- Paused: 暂停状态，运行出错会进入此状态，可以通过 `resume-task` 命令重新开始运行任务
- Stopped: 停止状态，不可通过 `resume-task` 重新运行。在 Running 或 Paused 状态通过 `stop-task` 会进入此状态
- Finished: 运行结束状态，只有全量任务正常运行结束会进入此状态

#### 状态转换图

```
                                         error occured
                            New -------------------------------|
                             |                                 |
                             |           resume task           |
                             |  |---------------------------|  |
                             |  |                           |  |
                             |  |                           |  |
                             v  v        error occured      |  v
  Finished <-------------- Running ----------------------> Paused
                             ^  |                           |
                             |  |                           |
                  start task |  | stop task                 |
                             |  |                           |
                             |  v        stop task          |
                           Stopped <------------------------|

```
