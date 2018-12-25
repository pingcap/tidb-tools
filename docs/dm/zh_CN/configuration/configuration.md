Task 配置文件介绍
===

task 配置文件 [task.yaml](./task.yaml) 主要包含下面 [全局配置](#全局配置) 和 [instance 配置](#instance-配置) 两部分。

具体各配置项的功能，可以阅读 [同步功能介绍](../overview.md#同步功能介绍)


### 关键概念

| 概念         | 解释                                                         | 配置文件                                                     |
| ------------ | ------------------------------------------------------------ | ------------------------------------------------------------ |
| source-id  | 唯一确定一个 MySQL / MariaDB 实例, 或者一个具有主从结构的复制组 | `inventory.ini` 的 `source_id`;<br> `dm-master.toml` 的 `source-id`;<br> `task.yaml` 的 `source-id` |
| DM-worker ID | 唯一确定一个 DM-worker （取值于 `dm-worker.toml` 的 `worker-addr` 参数） | `dm-worker.toml` 的 `worker-addr`;<br> dmctl 命令行的 `-worker` / `-w` flag  |


### 全局配置

#### 基础信息配置

```
name: test                      # 任务名称，需要全局唯一
task-mode: all                  # 任务模式，full / incremental / all
is-sharding: true               # 是否为分库分表任务
meta-schema: "dm_meta"          # 下游储存 meta 信息的 database
remove-meta: false              # 是否在任务同步开始前移除上面的 meta(checkpoint, onlineddl)
enable-heartbeat: false         # 是否开启 heartbeat 功能，具体解释见同步功能 heartbeat 介绍

target-database:                # 下游数据库实例配置
    host: "192.168.0.1"
    port: 4000
    user: "root"
    password: ""
```

##### 任务模式 - task-mode

`task-mode`: string( `full` / `incremental` / `all` ); 默认为 `all`

解释：任务模式，可以通过任务模式来指定需要执行的数据迁移工作
- `full` - 只全量备份上游数据库，然后全量导入到下游数据库
- `incremental` - 只通过 binlog 把上游数据库的增量修改同步到下游数据库, 可以设置 instance 配置的 meta 配置项来指定增量同步开始的位置
- `all` - `full` + `incremental`，先全量备份上游数据库，导入到下游数据库，然后从全量数据备份时导出的位置信息（binlog position / GTID）开始通过 binlog 增量同步数据到下游数据库


#### 功能配置集

主要包含下面几个功能配置集

```
routes:                                             # 上游和下游表之间的路由映射规则集
    route-rule-1:
    ​    schema-pattern: "test_*"                
    ​    table-pattern: "t_*"
    ​    target-schema: "test"
    ​    target-table: "t"
    route-rule-2:
    ​    schema-pattern: "test_*"
    ​    target-schema: "test"

filters:                                            # 上游数据库实例的匹配的表的 binlog 过滤规则集
    filter-rule-1:
    ​    schema-pattern: "test_*"
    ​    table-pattern: "t_*"
    ​    events: ["truncate table", "drop table"]
    ​    action: Ignore

black-white-list:                                   # 该上游数据库实例的匹配的表的黑白名单过滤规则集
    bw-rule-1:
    ​    do-dbs: ["~^test.*", "do"]
    ​    ignore-dbs: ["mysql", "ignored"]
    ​    do-tables:
    ​    - db-name: "~^test.*"
    ​      tbl-name: "~^t.*"

column-mappings:                                    # 上游数据库实例的匹配的表的列值转换规则集
    cm-rule-1:
    ​    schema-pattern: "test_*"
    ​    table-pattern: "t_*"
    ​    expression: "partition id"
    ​    source-column: "id"
    ​    target-column: "id"
    ​    arguments: ["1", "test_", "t_"]

mydumpers:                                          # mydumper 处理单元运行配置参数
    global:
    ​    mydumper-path: "./mydumper"                 # mydumper binary 文件地址，这个无需设置，会由 ansible 部署程序自动生成
    ​    threads: 16                                 # mydumper 从上游数据库实例 dump 数据的线程数量
    ​    chunk-filesize: 64                          # mydumper 生成的数据文件大小
    ​    skip-tz-utc: true						
    ​    extra-args: "-B test -T t1,t2 --no-locks"

loaders:                                            # loader 处理单元运行配置参数
    global:
    ​    pool-size: 16                               # loader 并发执行 mydumper 的 SQLs file 的线程数量
    ​    dir: "./dumped_data"                        # loader 读取 mydumper 输出文件的地址，同实例对应的不同任务应该不同 （mydumper 会根据这个地址输出 SQLs 文件）

syncers:                                            # syncer 处理单元运行配置参数
    global:
    ​    worker-count: 16                            # syncer 并发同步 binlog events 的线程数量
    ​    batch: 1000                                 # syncer 同步到下游数据库的一个事务批次 SQL 数
    ​    max-retry: 100                              # syncer 同步到下游数据库出错的事务的重试次数（仅限于 DML）
```


### instance 配置

定义具体的数据同步子任务，支持从单个或者多个上游 MySQL 实例同步到同一个下游数据库实例。

```
mysql-instances:
    -
    ​    source-id: "mysql-replica-01"           # 上游实例或者复制组 ID，参考 inventory.ini 的 source_id 或者 dm-master.toml 的 source-id 配置
    ​    meta:                                   # 下游数据库的 checkpoint 不存在时 binlog 同步开始的位置; 如果 checkpoint 存在则以 checkpoint 为准
    ​        binlog-name: binlog-00001
    ​        binlog-pos: 4

    ​    route-rules: ["route-rule-1", "route-rule-2"]    # 该上游数据库实例匹配的表到下游数据库的映射规则名称
    ​    filter-rules: ["filter-rule-1"]                  # 该上游数据库实例匹配的表的 binlog 过滤规则名称
    ​    column-mapping-rules: ["cm-rule-1"]              # 该上游数据库实例匹配的表的列值转换规则名称
    ​    black-white-list:  "bw-rule-1"                   # 该上游数据库实例匹配的表的黑白名单过滤规则名称

    ​    mydumper-config-name: "global"          # mydumper 配置名称
    ​    loader-config-name: "global"            # loader 配置名称
    ​    syncer-config-name: "global"            # syncer 配置名称

    -
    ​    source-id: "mysql-replica-02"           # 上游实例或者复制组 ID，参考 inventory.ini 的 source_id 或者 dm-master.toml 的 source-id 配置
    ​    mydumper-config-name: "global"          # mydumper 配置名称
    ​    loader-config-name: "global"            # loader 配置名称
    ​    syncer-config-name: "global"            # syncer 配置名称
```
