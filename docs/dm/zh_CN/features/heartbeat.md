heartbeat
===

#### 功能介绍

heartbeat 提供根据真实同步数据来计算每个同步任务与 MySQL/Mariadb 实时同步延迟的功能。

注意：
- 同步延迟的估算的精读在秒级别
- heartbeat 相关的 binlog 不会同步到下游，在计算延迟后会被丢弃

#### 系统权限
如果开启 heartbeat 功能，需要上游 MySQL/MariaDB 实例提供下面的权限

* SELECT
* INSERT
* CREATE（databases, tables）

#### 参数配置

task 配置文件中设置
```
enable-heartbeat: true
```

#### 原理介绍

* DM-worker 在对应的上游 MySQL/MariaDB 创建库 `dm_heartbeat` (当前不可配置)
* DM-worker 在对应的上游 MySQL/MariaDB 创建表 `heartbeat` (当前不可配置)
* DM-worker 每秒钟（当前不可配置）在对应的上游 MySQL/MariaDB 的 `dm_heartbeat`.`heartbeat` 表中，利用 `replace statement` 更新当前时间戳 `TS_master`
* DM-worker 每个任务拿到 `dm_heartbeat`.`heartbeat` 的 binlog 后，更新自己的同步时间 `TS_slave_task`
* DM-worker 每 10 秒在对应的上游 MySQL/MariaDB 的 `dm_heartbeat`.`heartbeat` 查询当前的 `TS_master`, 并且对每个任务计算 `task_lag` = `TS_master` - `TS_slave_task`

可以在 metrics 的 binlog replication 处理单元找到该监控 [replicate lag](../maintenance/metrics-alert.md#binlog-replication)