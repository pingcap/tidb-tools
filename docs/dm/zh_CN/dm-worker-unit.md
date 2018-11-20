DM-Worker 详细介绍
===

### 简介
- DM-Worker 可以链接到一台 MySQL/MariaDB，并且注册为该实例的 Slave
- 支持读取 MySQL/MariaDB Binlog 持久化保存在本地（relay log）
- 单个 DM-worker 支持同步一个上游 MySQL/MariaDB 到多个下游 TiDB
- 多个 DM-Worker 支持同步多个 MySQL/MariaDB 到一个下游 TiDB 

注意：如果需要合并多个上游 MySQL/MariaDB 的表到一张下游 TiDB 的表请参考 [分库分表](./shard-table))

### DM-Worker 处理单元
DM-Worker 任务运行过程包含多个任务处理逻辑单元

#### relay log
持久化保存从上游 MySQL/MariaDB 读取的 Binlog，并且对 binlog replication 处理单元提供读取 Binlog events 的功能

原理和功能与 MySQL slave relay log 类似，参见 <https://dev.mysql.com/doc/refman/5.7/en/slave-logs-relaylog.html>。

#### dumper
从上游 MySQL/MariaDB dump 全量数据到本地磁盘

#### loader
读取 dump unit 的数据文件，然后加载到下游 TiDB

#### binlog replication/syncer
读取 relay log unit 的 Binlog events，转化为 SQLs，然后应用到下游 TiDB



### DM-Worker 需要的权限
包含 relay log、dump、load、replicate binlog 等处理单元， 这里先总体说下 上下游分别需要什么权限；

#### 上游（MySQL/mariaDB)
SELECT
RELOAD
RELICATION SLAVE
REPLICATION CLIENT

```sql
GRANT SELECT,RELOAD,REPLICATION SLAVE, REPLICATION CLIENT  ON *.* TO 'your_user'@'your_wildcard_of_host';
```

#### 下游 (TiDB)
SELECT 
INSERT
UPDATE
DELETE
CREATE
DROP
ALTER
INDEX
即：

```sql
GRANT SELECT,INSERT,UPDATE,DELETE,CREATE,DROP,ALTER,INDEX  ON *.* TO 'your_user'@'your_wildcard_of_host';
```

下面会对各处理单元进行权限的细分（注意，随着需求的变化，这些权限也会跟着变化，并非一成不变）


### 处理单元需要的最小权限

| 处理单元 | 上游 (MySQL/MariaDB) | 下游 (TiDB) | 系統 |
|----:|:--------------------|:------------|:----|
|relay log |SELECT（查询上游的一些环境变量，比如 binlog_format）<br>REPLICATION SLAVE（读取 binlog）<br>REPLICATION CLIENT（show master status, show slave status）| 无 | 本地读/写磁盘 |
|dumper |SELECT<br>RELOAD（flush tables with read lock, unlock tables）| 无 | 本地写磁盘 |
|loader | 无 |SELECT（查询 checkpoint 记录）<br>CREATE（create database/create table）<br>DELETE（delete checkpoint）<br>INSERT（插入 dump 数据）| 本地读/写磁盘 |
|binlog replication |SELECT（查询上游的一些环境变量，比如 binlog_format）<br>REPLICATION SLAVE（读取 binlog）<br>REPLICATION CLIENT（show master status, show slave status）| SELECT（show index, show column）<br>INSERT（dml）<br>UPDATE（dml）<br>DELETE（dml）<br>CREATE（databases, tables）<br>DROP （databases, tables）<br>ALTER（alter table）<br>INDEX（create/drop index）| 本地读/写磁盘 |
