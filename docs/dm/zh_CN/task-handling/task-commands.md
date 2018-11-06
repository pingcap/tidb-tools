任务管理命令
===

### 创建数据同步任务

创建任务的时候，会对上游数据库实例的权限，以及表结构进行检查，对于分库分表同步任务的所有分表的表结构行如下的检查
- 表中是否存在自增并且唯一的 column，并且是否存在对应的 `partition id` 类型 column mapping rule 且是否存在冲突
- 上下游待同步表结构是否一致

```
» help start-task
start a task with config file

Usage:
 dmctl start-task [-w worker ...] <config_file> [flags]

Flags:
 -h, --help   help for start-task

Global Flags:
 -w, --worker strings   dm-worker ID
```

#### 命令样例

`start-task [ -w "172.16.30.15:10081"] ./task.yaml`

#### 参数解释

`-w`: 指定在特定的一组 dm-workers 执行上 `task.yaml`，可选。如果设置，则只启动指定任务在该 dm-workers 上面的子任务

`config_file`: `task.yaml` 文件路径，必选

#### 返回结果

```
{
​    "result": true,
​    "msg": "",
​    "workers": [
​        {
​            "result": true,
​            "worker": "172.16.30.15:10081",
​            "msg": ""
​        },
​        {
​            "result": true,
​            "worker": "172.16.30.16:10081",
​            "msg": ""
​        }
​    ]
}
```

### 查询数据同步任务状态

```
» help query-status
query task's status

Usage:
 dmctl query-status [-w worker ...] [task_name] [flags]

Flags:
 -h, --help   help for query-status

Global Flags:
 -w, --worker strings   dm-worker ID
```


#### 命令样例

`query-status`

#### 参数解释

`-w`: 查询在特定的一组 `dm-works` 上运行的数据同步任务的子任务，可选

`task_name`: task 任务名称，可选，如果不填写则返回全部数据同步任务

#### 返回结果

```
» query-status
{
​    "result": true,
​    "msg": "",
​    "workers": [
​        {
​            "result": true,
​            "worker": "172.16.30.15:10081",
​            "msg": "",
​            "subTaskStatus": [
​                {
​                    "name": "test",
​                    "stage": "Running",
​                    "unit": "Sync",
​                    "result": null,
​                    "unresolvedDDLLockID": "",
​                    "sync": {
​                        "TotalEvents": "0",
​                        "TotalTps": "0",
​                        "RecentTps": "0",
​                        "MasterBinlog": "(mysql-bin.000004, 484)",
​                        "MasterBinlogGtid": "",
​                        "SyncerBinlog": "(mysql-bin.000004, 484)",
​                        "SyncerBinlogGtid": "",
                        "blockingDDLs": [
                        ],
                        "unresolvedGroups": [
                        ]
​                    }
​                }
​            ],
​            "relayStatus": {
​                "MasterBinlog": "(mysql-bin.000004, 484)",
​                "MasterBinlogGtid": "",
                "relaySubDir": "0-1.000001",
​                "RelayBinlog": "(mysql-bin.000004, 484)",
​                "RelayBinlogGtid": "",
                "relayCatchUpMaster": true,
                "stage": "Running",
                "result": null
​            }
​        },
​        {
​            "result": true,
​            "worker": "172.16.30.16:10081",
​            "msg": "",
​            "subTaskStatus": [
​                {
​                    "name": "test",
​                    "stage": "Running",
​                    "unit": "Sync",
​                    "result": null,
​                    "unresolvedDDLLockID": "",
​                    "sync": {
​                        "TotalEvents": "0",
​                        "TotalTps": "0",
​                        "RecentTps": "0",
​                        "MasterBinlog": "(mysql-bin.000004, 4809)",
​                        "MasterBinlogGtid": "",
​                        "SyncerBinlog": "(mysql-bin.000004, 4809)",
​                        "SyncerBinlogGtid": "",
                        "blockingDDLs": [
                        ],
                        "unresolvedGroups": [
                        ]
​                    }
​                }
​            ],
​            "relayStatus": {
​                "MasterBinlog": "(mysql-bin.000004, 4809)",
​                "MasterBinlogGtid": "",
                "relaySubDir": "0-1.000001",
​                "RelayBinlog": "(mysql-bin.000004, 4809)",
​                "RelayBinlogGtid": "",
                "relayCatchUpMaster": true,
                "stage": "Running",
                "result": null
​            }
​        }
​    ]
}
```

### 暂停数据同步任务

```
» help pause-task
pause a running task with name

Usage:
 dmctl pause-task [-w worker ...] <task_name> [flags]

Flags:
 -h, --help   help for pause-task

Global Flags:
 -w, --worker strings   dm-worker ID
```

#### 命令样例

`pause-task [-w "127.0.0.1:10181"] task-name`

#### 参数解释

`-w`: 指定在特定的一组 dm-workers 暂停数据同步任务的子任务，可选。如果指定，则只暂停该任务在指定 dm-workers 上面的子任务

`task-name`: task 的名字，必选

#### 返回结果

```
» pause-task test
{
​    "op": "Pause",
​    "result": true,
​    "msg": "",
​    "workers": [
​        {
​            "op": "Pause",
​            "result": true,
​            "worker": "172.16.30.15:10081",
​            "msg": ""
​        },
​        {
​            "op": "Pause",
​            "result": true,
​            "worker": "172.16.30.16:10081",
​            "msg": ""
​        }
​    ]
}
```

### 重启数据同步任务

```
» help resume-task
resume a paused task with name

Usage:
 dmctl resume-task [-w worker ...] <task_name> [flags]

Flags:
 -h, --help   help for resume-task

Global Flags:
 -w, --worker strings   dm-worker ID
```

#### 命令样例

`resume-task [-w "127.0.0.1:10181"] task-name`

#### 参数解释

`-w`: 指定在特定的一组 dm-workers 重启数据同步任务的子任务，可选。如果指定，则只重启该任务在指定 dm-workers 的子任务

`task-name`: task 任务名称，必选

#### 返回结果

```
» resume-task test
{
​    "op": "Resume",
​    "result": true,
​    "msg": "",
​    "workers": [
​        {
​            "op": "Resume",
​            "result": true,
​            "worker": "172.16.30.15:10081",
​            "msg": ""
​        },
​        {
​            "op": "Resume",
​            "result": true,
​            "worker": "172.16.30.16:10081",
​            "msg": ""
​        }
​    ]
}
```

### 停止数据同步任务

```
» help stop-task
stop a task with name

Usage:
 dmctl stop-task [-w worker ...] <task_name> [flags]

Flags:
 -h, --help   help for stop-task

Global Flags:
 -w, --worker strings   dm-worker ID
```

#### 命令样例

`stop-task [-w "127.0.0.1:10181"]  task-name`

#### 参数解释

`-w`: 指定在特定的一组 dm-workers 停止数据同步任务的子任务，可选。如果指定，则只停止指定任务在该 dm-workers 上面的子任务

`task-name`: task 任务名称，必选

#### 返回结果

```
» stop-task test
{
​    "op": "Stop",
​    "result": true,
​    "msg": "",
​    "workers": [
​        {
​            "op": "Stop",
​            "result": true,
​            "worker": "172.16.30.15:10081",
​            "msg": ""
​        },
​        {
​            "op": "Stop",
​            "result": true,
​            "worker": "172.16.30.16:10081",
​            "msg": ""
​        }
​    ]
}
```

### 更新数据同步任务

#### 注意

##### 支持的更新项

- table route rules
- black white list
- binlog filter rules
- column mapping  rules

##### 支持更新项更新步骤

- 通过 `query-status <task-name>` 查看对应的数据同步任务状态
    - `stage` 不为 `Paused`，则通过 `pause-task <task-name>` 暂停任务
- 在 `task.yaml` 中更新用户需要修改的自定义配置或者错误配置
- 使用 `update-task task.yaml` 更新任务配置
- `resume-task <task-name>` 恢复任务

##### 不支持更新项更新步骤

- 通过 `query-status <task-name>` 查看对应的数据同步任务状态
    - 如果任务存在，则通过 `stop-task <task-name>` 停止任务
- 更新数据同步任务配置 `task.yaml`, 包含
    - 用户需要修改的自定义配置或者错误配置
- `start-task <task-name>` 重新开启任务

#### 命令 help

```
» help update-task
update a task's config for routes, filters, column-mappings, black-white-list

Usage:
  dmctl update-task [-w worker ...] <config_file> [flags]

Flags:
  -h, --help   help for update-task

Global Flags:
  -w, --worker strings   dm-worker ID
```

#### 命令样例

`update-task [-w "127.0.0.1:10181"] ./task.yaml`

#### 参数解释

`-w`: 指定在特定的一组 dm-workers 更新数据同步任务的子任务，可选。如果指定, 则只更新指定 dm-workers 上的子任务配置

`config file`: `task.yaml` 文件路径，必选

#### 返回结果

```
» update-task task_all_black.yaml
{
​    "result": true,
​    "msg": "",
​    "workers": [
​        {
​            "result": true,
​            "worker": "172.16.30.15:10081",
​            "msg": ""
​        },
​        {
​            "result": true,
​            "worker": "172.16.30.16:10081",
​            "msg": ""
​        }
​    ]
}
```
