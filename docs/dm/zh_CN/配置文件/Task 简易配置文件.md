Task 简易配置文件
===

样例: [dm.yaml](./dm.yaml)

### 注意

- 本配置文件适用于业务场景
    - 表特别多
    - 需要对不同的表设置不同的路由（不能用一个简单的 wildcard 表达式规则涵盖）
    - 需要设置不同过滤规则 （不能用一个简单的 wildcard 表达式规则涵盖）
- task 的功能项配置采用正则表达式或者 wildcard 的匹配方式，在业务配置复杂的情况下，无法简单的确认配置的正确性
- 本配置 **不采用** **正则表达式或者 wildcard 的匹配方式**，需要对每个表（或者某个确定的 schema）单独设置规则，所以会比 task 多更多的配置
- 目前可以通过这个方式配置的功能有
    - 分库分表关系配置
    - binlog event 过滤配置
    - 同步表黑白名单配置
- 具体的配置项解释见 [Task 配置项介绍]

### 关键概念

| 概念         | 解释                                                         | 配置文件                                                     |
| ------------ | ------------------------------------------------------------ | ------------------------------------------------------------ |
| instance-id  | 唯一确定一个 MySQL/MariaDB 实例（ansible 部署会用 host:port 来组装成该 ID） | dm-master.toml 的 mysql-instance; dm.yaml 的 instance-id; task.yaml 的 instance-id |
| dm-worker ID | 唯一确定一个 dm-worker （取值于dm-worker.toml 的 worker-addr 参数） | dm-worker.toml 的 worker-addr; dmctl 命令行的 -worker/-w flag  |

### 介绍

[dm.yaml](./dm.yaml) 里面包含一些与 [task.yaml](./task.yaml) 一样的基础配置信息，可以参考 task 文件介绍，下面介绍一下 DM 独有的配置项

- `do-tables`  
    要从上游同步的表，只需要按照下面的结构，把表名填写进去就可以构造正确的同步白名单  
    ```
    do-tables:
     instance118-4306:                              # mysql instance ID
       instance-id: instance118-4306       
       tables:
         schema_1:                                    # schema 下面要同步的表
         - table_1
         - table_2
         schema_2:
         - table_1
         - table_2
    ```
- `ignore-tables`  
    不从上游同步的表，结构同 `do-tables`
- `sharding-groups`  
    分库分表设置，主要包含
    - 各个实例的上游分表的集合
    - 各个实例的 column mapping rules 规则 （主要为了解决分库分表 ID 冲突）
    ```
    sharding-groups:
    - schema: test	     # 下游的目标库
      table: t	     # 下游的目标表
     source-tables:
       instance118-4306:           # MySQL Instance ID 为 `instance118-4306` 的分库分表配置
         instance-id: instance118-4306
         column-mapping-rules:                               # 该实例的 column-mapping-rules
         - schema-pattern: test_*
           table-pattern: t_*
           source-column: id
           target-column: id
           expression: partition id
           arguments:
           - "1"
           - schema_
           - table_
           create-table-query: ""
         tables:                                             # 该实例的分表集合
           schema_1:
           - table_1
           - table_2
           schema_2:
           - table_1
           - table_2
    ```

- `filter`  
    包含了binlog event 过滤规则配置
    ```
    filter:
     instance118-4306:                           # 对应的实例
       instance-id: instance118-4306        
       tables:
         table_1:                                 # 对应的 schema name
         - name: table_1                         # 对应的 table name
           ignored-binlog-events:   # 需要忽略掉的 binlog events
           - truncate table
           - drop table
         - name: table_2
           ignored-binlog-events:
           - truncate table
           - drop table
         schema_2:
         - name: table_1
           ignored-binlog-events:
           - truncate table
           - drop table
         - name: table_2
           ignored-binlog-events:
           - truncate table
           - drop table
    ```
