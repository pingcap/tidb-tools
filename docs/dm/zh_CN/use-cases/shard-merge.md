分库分表合并场景
===

### 索引
- [场景描述](#场景描述)
   - [上游实例](#上游实例)
   - [同步需求](#同步需求)
   - [下游实例](#下游实例)
- [同步方案](#同步方案)
- [任务配置](#任务配置)

### 场景描述

描述一个分库分表合并的业务场景，将上游三个 MySQL 实例的分库和分表同步到下游一个 TiDB 集群中。

#### 上游实例

假设上游存在下面的 schema

- instance 1

| schma | tables|
|------:|:------|
| user  | information, log_north, log_bak|
| store_01 | sale_01, slae_02 |
| store_02 | sale_01, slae_02 |

- instance 2

| schma | tables|
|------:|:------|
| user  | information, log_east, log_bak|
| store_01 | sale_01, slae_02 |
| store_02 | sale_01, slae_02 |

- instance 3

| schma | tables|
|------:|:------|
| user  | information, log_south, log_bak|
| store_01 | sale_01, slae_02 |
| store_02 | sale_01, slae_02 |


#### 同步需求

1. 三个实例的 `user`.`information` 合并到下游 TiDB 的 `user`.`information`
2. 三个实例的 `user`.`log_{north|south|east}` 合并到下游 TiDB 的 `user`.`log_{north|south|east}`
3. 三个实例的 `store_{01|02}`.`sale_{01|02}` 合并到下游 TiDB 的 `store`.`sale`
4. 过滤掉三个实例的 `user``.log_{north|south|east}` 表的所有删除操作
5. 过滤掉三个实例的 `user`.`information` 表的所有删除操作
6. 过滤掉三个实例的 `store_{01|02}`.`sale_{01|02}` 表的所有删除操作
7. 过滤掉三个实例的 `user`.`log_bak`
8. `store_{01|02}`.`sale_{01|02}` 表存在 bigint 类型的自增主键，合并有冲突

#### 下游实例

假设同步到下游后的 schema

| schma | tables|
|------:|:------|
| user | information, log_north, log_east, log_south|
| store | sale |

### 同步方案

- 同步需求 1 和 2 通过设置 [table 路由](../features/table-route.md) 来解决，配置如下

```yaml
routes:
  ...
  user-route-rule:
    schema-pattern: "user"
    target-schema: "user"
```

***

- 同步需求 3 可以通过同步功能 [table 路由](../features/table-route.md) 来解决，配置如下

```yaml
routes:
  ...
  store-route-rule:
    schema-pattern: "store_*"
    target-schema: "store"
  sale-route-rule:
    schema-pattern: "store_*"
    table-pattern: "sale_*"
    target-schema: "store"
    target-table:  "sale"
```

***

- 同步需求 4 和 5 可以通过同步功能 [binlog 过滤](../features/binlog-filter.md) 来解决, 配置如下

```yaml
filters:
  ...
  user-filter-rule:
    schema-pattern: "user"
    events: ["truncate table", "drop table", "delete"， "drop database"]
    action: Ignore
```

注意： 同步需求 4 和 5， 加上需求 7 意味着可以过滤掉 schema `user` 的所有删除操作，所以这里设置一个 schema level 的过滤规则；但是以后有其他表，这些表的删除操作也会被过滤。

***

- 同步需求 6  可以通过同步功能 [binlog 过滤](../features/binlog-filter.md) 来解决，配置如下

```yaml
filters:
  ...
  sale-filter-rule:
    schema-pattern: "store_*"
    table-pattern: "sale_*"
    events: ["truncate table", "drop table", "delete"]
    action: Ignore
  store-filter-rule:
    schema-pattern: "store_*"
    events: ["drop database"]
    action: Ignore
```

***

- 同步需求 7 可以通过同步功能 [同步表黑白名单](../features/black-white-list.md) 来解决，配置如下

```yaml
black-white-list:
  log-bak-ignored:
    ignore-tales:
    - db-name: "user"
      tbl-name: "log_bak"
```

***

- 同步需求 8 可以通过同步功能 [列值转换](../features/column-mapping.md) 来解决，配置如下

```yaml
column-mappings:
  instance-1-sale:
​    schema-pattern: "store_*"
​    table-pattern: "sale_*"
​    expression: "partition id"
​    source-column: "id"
​    target-column: "id"
​    arguments: ["1", "store_", "sale_"]
  instance-2-sale:
​    schema-pattern: "store_*"
​    table-pattern: "sale_*"
​    expression: "partition id"
​    source-column: "id"
​    target-column: "id"
​    arguments: ["2", "store_", "sale_"]
  instance-3-sale:
​    schema-pattern: "store_*"
​    table-pattern: "sale_*"
​    expression: "partition id"
​    source-column: "id"
​    target-column: "id"
​    arguments: ["3", "store_", "sale_"]
```


### 任务配置

下面是完整的 task 配置, 详情见 [配置文件解释](../configuration/configuration.md)

```yaml
name: "shard_merge"
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
    route-rules: ["user-route-rule", "store-route-rule", "sale-route-rule"]
    filter-rules: ["user-filter-rule", "store-filter-rule" , "sale-filter-rule"]
    column-mapping-rules: ["instance-1-sale"]
    black-white-list:  "log-bak-ignored"
    mydumper-config-name: "global"
    loader-config-name: "global"
    syncer-config-name: "global"

  -
    source-id: "instance-2"
    route-rules: ["user-route-rule", "store-route-rule", "sale-route-rule"]
    filter-rules: ["user-filter-rule", "store-filter-rule" , "sale-filter-rule"]
    column-mapping-rules: ["instance-2-sale"]
    black-white-list:  "log-bak-ignored"
    mydumper-config-name: "global"
    loader-config-name: "global"
    syncer-config-name: "global"
  -
    source-id: "instance-3"
    route-rules: ["user-route-rule", "store-route-rule", "sale-route-rule"]
    filter-rules: ["user-filter-rule", "store-filter-rule" , "sale-filter-rule"]
    column-mapping-rules: ["instance-3-sale"]
    black-white-list:  "log-bak-ignored"
    mydumper-config-name: "global"
    loader-config-name: "global"
    syncer-config-name: "global"

# other common configs shared by all instances

routes:
  user-route-rule:
    schema-pattern: "user"
    target-schema: "user"
  store-route-rule:
    schema-pattern: "store_*"
    target-schema: "store"
  sale-route-rule:
    schema-pattern: "store_*"
    table-pattern: "sale_*"
    target-schema: "store"
    target-table:  "sale"

filters:
   user-filter-rule:
    schema-pattern: "user"
    events: ["truncate table", "drop table", "delete"， "drop database"]
    action: Ignore
  sale-filter-rule:
    schema-pattern: "store_*"
    table-pattern: "sale_*"
    events: ["truncate table", "drop table", "delete"]
    action: Ignore
  store-filter-rule:
    schema-pattern: "store_*"
    events: ["drop database"]
    action: Ignore

black-white-list:
  log-bak-ignored:
    ignore-tales:
    - db-name: "user"
      tbl-name: "log_bak"

column-mappings:
  instance-1-sale:
​    schema-pattern: "store_*"
​    table-pattern: "sale_*"
​    expression: "partition id"
​    source-column: "id"
​    target-column: "id"
​    arguments: ["1", "store_", "sale_"]
  instance-2-sale:
​    schema-pattern: "store_*"
​    table-pattern: "sale_*"
​    expression: "partition id"
​    source-column: "id"
​    target-column: "id"
​    arguments: ["2", "store_", "sale_"]
  instance-3-sale:
​    schema-pattern: "store_*"
​    table-pattern: "sale_*"
​    expression: "partition id"
​    source-column: "id"
​    target-column: "id"
​    arguments: ["3", "store_", "sale_"]


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