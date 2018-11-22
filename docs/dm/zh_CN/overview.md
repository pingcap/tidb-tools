DM 简介
===

### 介绍

DM (Data Migration) 是一体化数据同步任务管理平台，支持全量备份和 MariaDB/MySQL binlog 增量同步，设计的主要目的是
   - 标准化 （例如 工具运行，错误定义）
   - 降低运维使用成本
   - 简化错误处理流程
   - 提升产品使用体验


### 架构图

   ![DM structure](./architecture.png)


### 组件功能

#### DM-master

- 保存 DM 集群的拓扑信息
- 监控 DM-worker 进程的运行
- 监控数据同步任务的运行状态
- 提供数据同步任务管理的统一入口
- 协调 sharding 场景下各个实例的分表 DDL 同步

#### DM-worker

- 支持 binlog 本地持久化
- 保存数据同步子任务的配置信息
- 编排数据同步子任务的运行
- 监控数据同步子任务的运行状态

参考 [DM-worker 详细介绍](./dm-worker-unit.md)

#### dmctl

- 创建 / 更新 / 删除数据同步任务
- 查看数据同步任务状态
- 处理数据同步任务错误
- 校验数据同步任务配置的正确性

### 同步功能介绍

#### schema / table 同步黑白名单

上游数据库实例表的黑白名过滤名单规则。过滤规则类似于 MySQL replicarion-rules-db / tables, 可以用来设置过滤或者只同步某些 database 或者 table 的所有操作。详情见 [Task 配置项介绍](./configuration/argument-explanation.md)

#### binlog Event 过滤

比 `schema / table 同步黑白名单` 更加细粒度的过滤规则，可以指定只同步或者过滤掉某些 database 或者 table 的具体的 Binlog events，比如 `INSERT`，`TRUNCATE TABLE`。详情见 [Task 配置项介绍](./configuration/argument-explanation.md)

#### column mapping

解决分库分表存在自增主键 ID 的冲突，根据用户配置的 instance-id 以及 schema / table 的 ID 来对自增主键 ID 的值进行转换。详情见 [Task 配置项介绍](./configuration/argument-explanation.md)

#### 分库分表支持

DM 支持对分库分表进行合并到下游 TiDB 一张表需求，但需要满足一些限制，详情见 [分库分表](shard-table)
