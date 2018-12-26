DM 同步任务前置检查
===

为了提前发现数据同步任务的一些配置错误，DM 中增加了前置检查功能来预防

### 检查项

#### MySQL binlog 配置

- binlog 是否开启（DM 要求 binlog 必须开启）;
- 是否 `binlog_format=ROW`; (DM 只支持 ROW format binlog 的同步)
- 是否 `binlog_row_image=FULL`;  (DM 只支持 `binlog_row_image=FULL`)

#### 上游 MySQL 实例权限

DM 配置中的 MySQL user 至少需要具有以下权限

- REPLICATION SLAVE
- REPLICATION CLIENT
- RELOAD
- SELECT

#### 上游 MySQL 表结构兼容性

TiDB 和 MySQL 的兼容性存在一些差别

- foreign key 不支持
- 字符集的兼容性 <https://github.com/pingcap/docs-cn/blob/master/sql/character-set-support.md>

#### 上游 MySQL 多实例分库分表一致性

- 所有分表的表结构是否一致，检查内容包括
    - column 数量
    - column 名称
    - column 位置
    - column 类型
    - 主键
    - 唯一索引
- 所有分表的是否存在自增主键
    - 如果存在自增主键，自增主键 column 类型为 bigint，并且为其配置了 [column mapping](./features/column-mapping.md)，那么检查允许通过
    - 否则任务检查不通过
