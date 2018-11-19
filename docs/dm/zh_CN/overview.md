DM 简介
===

### 介绍

DM (Data Migration) 是一体化数据同步任务管理平台，支持全量备份和 Mariadb/MySQL binlog 增量同步，设计的主要目的是
   - 标准化 （e.g. 工具运行，错误定义）
   - 降低运维使用成本
   - 简化错误处理流程
   - 提升产品使用体验


### 架构图

   ![DM structure](./architecture.png)


### 组件功能

#### dm-master

- 保存 DM 集群的拓扑信息
- 监控 dm-worker 进程的运行
- 监控数据同步任务的运行状态
- 提供数据同步任务管理的统一入口
- 协调 sharding 场景下各个实例的分表 DDL 同步

#### dm-worker

- 链接到一台 MySQL/MariDB，并且注册为该实例的 Slave
- 读取 MySQL/MariaDB Binlog 持久化保存再本地（relay log）
- 支持同步到多个下游 TiDB
- 保存数据同步子任务的配置信息
- 编排数据同步子任务的运行
- 监控数据同步子任务的运行状态

#### dmctl

- 创建 / 更新 / 删除数据同步任务
- 查看数据同步任务状态
- 处理数据同步任务错误
- 校验数据同步任务配置的正确性


### DM-Worker 处理单元

#### relay log
持久化保存从上游 MySQL/MariaDB 读取的 Binlog，并且对 binlogreplication unit 提供读取 Binlog events 的功能

#### dump
从上游 MySQL/MariaDB dump 全量数据到本地磁盘

#### load
读取 dump unit 的数据文件，然后加载到下游 TiDB

#### binlog replication
读取 relay log unit 的 Binlog events，转化为 SQLs，然后应用到下游 TiDB

#### 权限要求
参考[权限说明文档](./privileges.md)