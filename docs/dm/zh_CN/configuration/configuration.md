Task 配置文件介绍
===

task 配置文件 [task.yaml](./task.yaml) 主要包含下面 [全局配置](#全局配置) 和 [instance 配置](#instance-配置) 两部分。

具体各配置项的解释，可以阅读 [Task 配置项介绍](./argument-explanation.md)。


### 关键概念

instance-id，dm-worker ID 等关键概念参见 [关键概念](../user-manual.md#关键概念) 


### 全局配置

#### 基础信息配置

```
name: test                      # 任务名称，需要全局唯一
task-mode: all                  # 任务模式，full / incremental / all，详情见 [Task 配置项介绍]
is-sharding: true               # 是否为分库分表任务
meta-schema: "dm_meta"          # 下游储存 meta 信息的 database
remove-meta: false              # 是否在任务同步开始前移除上面的 meta(checkpoint, onlineddl)

target-database:                # 下游数据库实例配置
    host: "192.168.0.1"
    port: 4000
    user: "root"
    password: ""
```

#### 功能配置集

主要包含下面几个功能配置集

```
routes:                                             # 上游和下游表之间的路由映射规则集
    user-route-rules-schema:                        # schema-pattern / table-pattern 采用 wildcard 的匹配规则，具体见 [Task 配置项介绍] 文档的 [route rule] 解释
    ​    schema-pattern: "test_*"                
    ​    table-pattern: "t_*"
    ​    target-schema: "test"
    ​    target-table: "t"

filters:                                            # 上游数据库实例的匹配的表的 binlog event 过滤规则集
    user-filter-1:                                  # 具体见 [Task 配置项介绍] 文档的 [binlog event filter rule] 解释
    ​    schema-pattern: "test_*"
    ​    table-pattern: "t_*"
    ​    events: ["truncate table", "drop table"]
    ​    action: Ignore

black-white-list:                                   # 该上游数据库实例的匹配的表的黑名过滤名单规则集
    instance:                                       # 具体解释见 [Task 配置项介绍] 文档的 [black white list rule] 解释
    ​    do-dbs: ["~^test.*", "do"]
    ​    ignore-dbs: ["mysql", "ignored"]
    ​    do-tables:
    ​    - db-name: "~^test.*"
    ​      tbl-name: "~^t.*"

column-mappings:                                    # 上游数据库实例的匹配的表的 column 映射规则集
    instance-1:                                     # 限制和具体解释见 [Task 配置项介绍] 文档的 [column mapping rule] 解释
    ​    schema-pattern: "test_*"
    ​    table-pattern: "t_*"
    ​    expression: "partition id"
    ​    source-column: "id"
    ​    target-column: "id"
    ​    arguments: ["1", "test_", "t_"]

mydumpers:                                          # mydumper 组件运行配置参数
    global:
    ​    mydumper-path: "./mydumper"                 # mydumper binary 文件地址，这个无需设置，会由 ansible 部署程序自动生成
    ​    threads: 16                                 # mydumper 从上游数据库实例 dump 数据的线程数量
    ​    chunk-filesize: 64                          # mydumper 生成的数据文件大小
    ​    skip-tz-utc: true						
    ​    extra-args: "-B test -T t1,t2 --no-locks"

loaders:                                            # loader 组件运行配置参数
    global:
    ​    pool-size: 16                               # loader 并发执行 mydumper 的 SQLs file 的线程数量
    ​    dir: "./dumped_data"                        # loader 读取 mydumper 输出文件的地址，同实例对应的不同任务应该不同 （mydumper 会根据这个地址输出 SQLs 文件）

syncers:                                            # syncer 组件运行配置参数
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
    ​    config:                                    # instance-id 对应的上游数据库配置 
    ​        host: "192.168.199.118"
    ​        port: 4306
    ​        user: "root"
    ​        password: "1234"                       # 需要使用 dmctl 加密的密码，具体说明见 [dmctl 使用手册]
    ​    instance-id: "instance118-4306"            # MySQL Instance ID，对应上游 MySQL 实例，不允许配置 dm-master 的集群拓扑配置外的 mysql-instance 

    ​    meta:                                      # 下游数据库的 checkpoint 不存在时 binlog 同步开始的位置; 如果 checkpoint 存在则没有作用 
    ​        binlog-name: binlog-00001
    ​        binlog-pos: 4

    ​    route-rules: ["user-route-rules-schema", "user-route-rules"]       # 该上游数据库实例匹配的表到下游数据库的映射规则名称，具体配置见 [全局配置] [功能配置项集] 的 routes
    ​    filter-rules: ["user-filter-1", "user-filter-2"]                   # 该上游数据库实例匹配的表的 binlog event 过滤规则名称，具体配置见 [全局配置] [功能配置项集] 的 filters
    ​    column-mapping-rules: ["instance-1"]                               # 该上游数据库实例匹配的表的 column 映射规则名称，具体配置见 [全局配置] [功能配置项集] 的 column-mappings
    ​    black-white-list:  "instance"                                      # 该上游数据库实例匹配的表的黑名过滤名单规则名称，具体配置见 [全局配置] [功能配置项集] 的 black-white-list

    ​    mydumper-config-name: "global"                                     # mydumper 配置名称，具体配置见 [全局配置] [功能配置项集] 的 mydumpers (不能和 mydumper 同时设置）
    ​    loader-config-name: "global"                                       # loader 配置名称，具体配置见 [全局配置] [功能配置项集] 的 loaders（不能和 loader 同时设置）
    ​    syncer-config-name: "global"                                       # syncer 配置名称，具体配置见 [全局配置] [功能配置项集] 的 syncers （不能和 syncer 同时设置）

    -
    ​    config:
    ​        host: "192.168.199.118"
    ​        port: 5306
    ​        user: "root"
    ​        password: "1234"
    ​    instance-id: "instance118-5306"

    ​    mydumper:                                                          # mydumper 相关配置（不能和 mydumper-config-name 同时设置）
    ​        mydumper-path: "./mydumper"                                    # mydumper binary 文件地址，这个无需设置，会由 ansible 部署程序自动生成
      ​      threads: 4
    ​        chunk-filesize: 8
    ​        skip-tz-utc: true
      ​      extra-args: "-B test -T t1,t2"
    
    ​    loader:                                                            # loader 相关配置（不能和 loader-config-name 同时设置）
      ​      pool-size: 32                                                  # 见上文 loaders 配置项说明
      ​      dir: "./dumped_data"
    
    ​    syncer:                                                            # syncer 相关配置（不能和 syncer-config-name 同时设置）
     ​       worker-count: 32                                               # 见上文 syncers 配置项说明
     ​       batch: 2000
    ​        max-retry: 200
```
