DM-Worker 详细介绍
===

### 简介
- DM-Worker 可以链接到一台 MySQL/MariDB，并且注册为该实例的 Slave
- 支持读取 MySQL/MariaDB Binlog 持久化保存再本地（relay log）
- 支持同步到多个下游 TiDB
- 支持多个 DM-Worker 同步多个 MySQL/MariaDB 到一个下游 TiDB （此外如果需要合并多个表到一张表请参考 [分库分表](./shard-table))

### DM-Worker 处理单元
DM-Worker 包含多个任务处理逻辑单元

#### relay log
持久化保存从上游 MySQL/MariaDB 读取的 Binlog，并且对 binlogreplication unit 提供读取 Binlog events 的功能

#### dump
从上游 MySQL/MariaDB dump 全量数据到本地磁盘

#### load
读取 dump unit 的数据文件，然后加载到下游 TiDB

#### binlog replication
读取 relay log unit 的 Binlog events，转化为 SQLs，然后应用到下游 TiDB



### DM-Worker 需要的权限
包含 relay log，dump，load，replicate binlog 等任务运行单元， 这里先总体说下 上下游分别需要什么权限；

#### 上游（mysql/mariadb）
SELECT
RELOAD
RELICATION SLAVE
REPLICATION CLIENT

即 GRANT SELECT,RELOAD,REPLICATION SLAVE, REPLICATION CLIENT  ON *.* TO 'your_user'@'your_wildcard_of_host';

#### 下游 (tidb)
SELECT 
INSERT
UPDATE
DELETE
CREATE
DROP
ALTER
INDEX
即：GRANT SELECT,INSERT,UPDATE,DELETE,CREATE,DROP,ALTER,INDEX  ON *.* TO 'your_user'@'your_wildcard_of_host';

因为 DM 包含 dump，load，replicate binlog events 等组件，下面会对各组件进行权限的细分（注意，随着需求的变化，这些权限也会跟着变化，并非一成不变）



### units 需要的最小权限

#### relay log

###### 上游（mysql/mariadb）
SELECT （查询上游的一些环境变量，比如 binlog_format）
REPLICATION SLAVE (拉取 binlog)
REPLICATION CLIENT (show master status, show slave status)

###### 下游 (tidb)
无

###### 系统
本地读写磁盘权限



#### dump

###### 上游（mysql/mariadb）
SELECT 
RELOAD (flush tables with read lock, unlock tables)

###### 下游 (tidb)
无

###### 系统
本地写磁盘权限



#### load

###### 上游（mysql/mariadb）
无

###### 下游 (tidb)
SELECT （查询 checkpoint 记录）
CREATE (create database/create table)
DELETE （delete checkpoint）
INSERT (插入 ddump 数据)

###### 系统
本地读/写磁盘权限



#### binlog replication

###### 上游（mysql/mariadb）
SELECT （查询上游的一些环境变量，比如 binlog_format）
REPLICATION SLAVE (拉取 binlog)
REPLICATION CLIENT (show master status, show slave status)

###### 下游 (tidb)
SELECT (show index, show column）
INSERT (dml)
UPDATE (dml)
DELETE (dml)
CREATE （databases, tables, indexes）
DROP (databases, tables, )
ALTER (alter table)
INDEX (create/drop index)

###### 系统
本地读/写磁盘权限
