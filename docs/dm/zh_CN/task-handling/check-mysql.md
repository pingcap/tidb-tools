上游数据库实例检查
===

为了提前发现上游 MySQL 实例的错误配置，DM 中增加了预检查功能，支持

- 使用 dmctl 手动进行检查
- task 创建前，DM 会自动进行检查


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


#### 上游 MySQL 多实例分库分表一致性检测

- 表结构是否一致
    - column 名称，类型
    - 索引
- 是否存在在合并中会冲突的 auto increment primary key
