table route rule
===

#### 功能介绍

table route 提供将上游 MySQL/MariaDB 实例的某些表同步到下游指定表的功能。

注意：
- 不支持对同一个表设置多个不同的路由规则
- schema 的匹配规则需要单独设置，用来同步 `create/drop schema xx`, 例如下面的 `rule-2`

#### 参数配置

```
routes:                                             
  rule-1:
    schema-pattern: "test_*"                
    table-pattern: "t_*"
    target-schema: "test"
    target-table: "t"
  rule-2:
    schema-pattern: "test_*"
    target-schema: "test"
```

#### 参数解释

根据 [`schema-pattern` / `table-pattern`](./table-selector.md) 对匹配上该规则的上游 MySQL/MariaDB 实例的表同步到下游的 `tagrt-schema`/`target-table`

例子：

如果你需要同步 `test_[1,2,3]`.`t_[1,2,3]` 到下游的 `test`.`t`，则必须创建上面参数配置的两条路由规则 `rule-1` 和 `rule-2`
- `rule-1` 用来同步匹配上 `schema-pattern: "test_*"` 和 `table-pattern: "t_*"` 的表的 DMLs/DDLs 到下游的 `test`.`t` 
- `rule-2` 用来同步匹配上 `schema-pattern: "test_*"` 的库的 DDLs （`create/drop schema xx`）
