使用限制
===

### 数据库版本

- 5.5 < MySQL version < 5.8
- MariaDB version >= 10.1.2

另外，在使用 dmctl 启动任务时，DM 会对任务上下游数据库的配置、权限等进行前置检查，具体细节见参考 [同步任务前置检查](./prechek.md)

### DDL 语法

DM 使用 TiDB parser 解析处理 DDL statement，所以仅支持 TiDB parser 支持的 DDL 语法, 详情请参阅 [TiDB DDL 语法支持](https://github.com/pingcap/docs-cn/blob/master/sql/ddl.md)

### 分库分表

- 如果业务分库分表之间存在数据冲突，冲突的列*只有自增主键列*，并且*列的类型是 bigint*， 可以尝试使用 [column mapping](./features/column-mapping.md) 解决; 否则不支持同步，有冲突的数据会相互覆盖造成数据丢失

其他限制参见 [sharding DDL 使用限制](./shard-table/restrictions.md)

### 操作限制

- DM-worker 重启不能自动恢复 task，需要使用 dmctl 手动执行 `start-task`, 具体用法参阅 [dmctl 使用手册 ](./task-handling/dmctl-manual.md) 
- DM-worker / dm-master 重启后在一些情况下不能自动恢复 DDL lock 同步，需要手动处理， 具体步骤参照 [手动处理 sharding DDL lock](./shard-table/handle-DDL-lock.md)
