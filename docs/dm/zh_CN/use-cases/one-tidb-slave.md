简单的从库同步场景
===

### 索引
- [场景描述](#场景描述)
   - [上游实例](#上游实例)
   - [同步需求](#同步需求)
   - [下游实例](#下游实例)
- [同步方案](#同步方案)
- [任务配置](#任务配置)

### 场景描述

下面我们描述一个比较常见的场景，将上游三个 MySQL 实例同步到下游一个 TiDB 集群中（不涉及合并分表数据）。

#### 上游实例

假设上游存在下面的 schema

- instance 1

| schma | tables|
|------:|:------|
| user  | information, log|
| store | store_bj, store_tj |
| log   | messages |

- instance 2

| schma | tables|
|------:|:------|
| user  | information, log|
| store | store_sh, store_sz |
| log   | messages |

- instance 3

| schma | tables|
|------:|:------|
| user  | information, log|
| store | store_gz, store_sz |
| log   | messages |


#### 同步需求

1. schema `user` 不做合并
   1. instance 1 的 schema `user` 同步到 TiDB 的 `user_north`
   2. instance 2 的 schema `user` 同步到 TiDB 的 `user_east`
   3. instance 3 的 schema `user` 同步到 TiDB 的 `user_south`
   4. table `log` 不允许任删除操作
2. schema `store` 合并到下游的 `store`，表不合并
   1. instance 2 和 3 都存在 `store_sz`, 分别同步到 `store_suzhou`, `store_shenzhen`
   2. `store` 不允许任何删除操作
3. schema `log` 需要被过滤掉

#### 下游实例

假设同步到下游后的 schema

| schma | tables|
|------:|:------|
| user_north | information, log|
| user_east  | information, log|
| user_south | information, log|
| store | store_bj, store_tj, store_sh, store_suzhou, store_gz, store_shenzhen |

### 同步方案

- 同步需求 1 的 i、ii、iii 可以通过设置 table 路由来解决，配置如下

```yaml
routes:
  ...
  instance-1-user-rule:
    schema-pattern: "user"
    target-schema: "user_north"
  instance-2-user-rule:
    schema-pattern: "user"
    target-schema: "user_east"
  instance-3-user-rule:
    schema-pattern: "user"
    target-schema: "user_south"
```

***

- 同步需求 2 的 i 可以通过同步功能 [table 路由](../features/table-route.md) 来解决，配置如下

```yaml
routes:
  ...
  instance-2-store-rule:
    schema-pattern: "store"
    table-pattern: "store_sz"
    target-schema: "store"
    target-table:  "store_suzhou"
  instance-3-store-rule:
    schema-pattern: "store"
    table-pattern: "store_sz"
    target-schema: "store"
    target-table:  "store_shenzhen"
```

***

- 同步需求 1 的 iv 可以通过同步功能 [binlog 过滤](../features/binlog-filter.md) 来解决, 配置如下

```yaml
filters:
  ...
  log-filter-rule:
    schema-pattern: "user"
    table-pattern: "log"
    events: ["truncate table", "drop table", "delete"]
    action: Ignore
  user-filter-rule:
    schema-pattern: "user"
    events: ["drop database"]
    action: Ignore
```

***

- 同步需求 2  的 ii 可以通过同步功能 [binlog 过滤](../features/binlog-filter.md) 来解决，配置如下

```yaml
filters:
  ...
  store-filter-rule:
    schema-pattern: "store"
    events: ["drop database", "truncate table", "drop table", "delete"]
    action: Ignore
```

`filter-store-rule` 和 `filter-log-rule-table/schema` 的区别: `filter-store-rule` 针对整个 `store` schema, 而 `filter-log-rule-table/schema` 针对表 `user`.`log`

***

- 同步需求 3 可以通过同步功能 [同步表黑白名单](../features/black-white-list.md) 来解决，配置如下

```yaml
black-white-list:
  log-filter:
    ignore-dbs: ["log"]
```

### 任务配置

下面是完整的 task 配置, 详情见 [配置文件解释](../configuration/configuration.md)

```yaml
name: "one-tidb-slave"
task-mode: all
meta-schema: "dm_meta"
remove-meta: false

target-database:
  host: "192.168.0.1"
  port: 4000
  user: "root"
  password: ""

mysql-instances:
  -
    source-id: "instance-1"
    route-rules: ["instance-1-user-rule"]
    filter-rules: ["log-filter-rule", "user-filter-rule" , "store-filter-rule"]
    black-white-list:  "log-filter"
    mydumper-config-name: "global"
    loader-config-name: "global"
    syncer-config-name: "global"
  -
    source-id: "instance-2"
    route-rules: ["instance-2-user-rule", instance-2-store-rule]
    filter-rules: ["log-filter-rule", "user-filter-rule" , "store-filter-rule"]
    black-white-list:  "log-filter"
    mydumper-config-name: "global"
    loader-config-name: "global"
    syncer-config-name: "global"
  -
    source-id: "instance-3"
    route-rules: ["instance-3-user-rule", instance-3-store-rule]
    filter-rules: ["log-filter-rule", "user-filter-rule" , "store-filter-rule"]
    black-white-list:  "log-filter"
    mydumper-config-name: "global"
    loader-config-name: "global"
    syncer-config-name: "global"

# other common configs shared by all instances

routes:
  instance-1-user-rule:
    schema-pattern: "user"
    target-schema: "user_north"
  instance-2-user-rule:
    schema-pattern: "user"
    target-schema: "user_east"
  instance-3-user-rule:
    schema-pattern: "user"
    target-schema: "user_south"
  instance-2-store-rule:
    schema-pattern: "store"
    table-pattern: "store_sz"
    target-schema: "store"
    target-table:  "store_suzhou"
  instance-3-store-rule:
    schema-pattern: "store"
    table-pattern: "store_sz"
    target-schema: "store"
    target-table:  "store_shenzhen"

filters:
  log-filter-rule:
    schema-pattern: "user"
    table-pattern: "log"
    events: ["truncate table", "drop table", "delete"]
    action: Ignore
  user-filter-rule:
    schema-pattern: "user"
    events: ["drop database"]
    action: Ignore
  store-filter-rule:
    schema-pattern: "store"
    events: ["drop database", "truncate table", "drop table", "delete"]
    action: Ignore

black-white-list:
  log-filter:
    ignore-dbs: ["log"]

mydumpers:
  global:
    threads: 4
    chunk-filesize: 64
    skip-tz-utc: true

loaders:
  global:
    pool-size: 16
    dir: "./dumped_data"

syncers:
  global:
    worker-count: 16
    batch: 100
    max-retry: 100
```