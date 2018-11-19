DM 使用权限说明
===

### DM-Worker 需要的权限
包含 relay log，dump，load，replicate binlog 等 units， 这里先总体说下 上下游分别需要什么权限；

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

因为 DM 包含 dump，load，replicate binlog events 等组件，下面会对各组件进行权限的细分，方便大家深入理解（注意，随着需求的变化，这些权限也会跟着变化，并非一成不变）

### units 需要的最小权限

#### relay log unit

##### 上游（mysql/mariadb）
SELECT （查询上游的一些环境变量，比如 binlog_format）
REPLICATION SLAVE (拉取 binlog)
REPLICATION CLIENT (show master status, show slave status)

##### 下游 (tidb)
无

##### 系统
本地读写磁盘权限

#### dump unit

##### 上游（mysql/mariadb）
SELECT 
RELOAD (flush tables with read lock, unlock tables)

##### 下游 (tidb)
无

##### 系统
本地写磁盘权限


#### load unit

##### 上游（mysql/mariadb）
无

##### 下游 (tidb)
SELECT （查询 checkpoint 记录）
CREATE (create database/create table)
DELETE （delete checkpoint）
INSERT (插入 ddump 数据)

##### 系统
本地读/写磁盘权限

#### binlog replication unit

##### 上游（mysql/mariadb）
SELECT （查询上游的一些环境变量，比如 binlog_format）
REPLICATION SLAVE (拉取 binlog)
REPLICATION CLIENT (show master status, show slave status)

##### 下游 (tidb)
SELECT (show index, show column）
INSERT (dml)
UPDATE (dml)
DELETE (dml)
CREATE （databases, tables, indexes）
DROP (databases, tables, )
ALTER (alter table)
INDEX (create/drop index)

##### 系统
本地读/写磁盘权限
