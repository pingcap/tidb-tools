简单的从库同步场景
===

### 场景描述

下面我们描述一个比较常见的场景，将上游三个 MySQL 实例同步到下游一个 TiDB 集群中。 

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
  - instance 1 的 schema `user` 同步到 TiDB 的 `user_north`
  - instance 2 的 schema `user` 同步到 TiDB 的 `user_east`
  - instance 3 的 schema `user` 同步到 TiDB 的 `user_south`
  - table `log` 不允许任何删除操作
2. schema `store` 合并到下游的 `store`，表不合并
  - instance 2 和 3 都存在 `store_sz`, 分别同步到 `store_suzhou`, `store_shenzhen`
  - `store` 不允许任何删除操作
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



### 任务配置