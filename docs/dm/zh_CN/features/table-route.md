table 路由
===

### 索引
- [功能介绍](#功能介绍)
- [参数配置](#参数配置)
- [参数解释](#参数解释)
- [使用示例](#使用示例)
  - [分库分表合并](#分库分表合并)
  - [分库合并](#分库合并)
  - [错误的路由](#错误的路由)

### 功能介绍

table route 提供将上游 MySQL/MariaDB 实例的某些表同步到下游指定表的功能。

注意：
- 不支持对同一个表设置多个不同的路由规则
- schema 的匹配规则需要单独设置，用来同步 `create/drop schema xx`, 例如下面的 `rule-2`

### 参数配置

```yaml
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

### 参数解释

根据 [`schema-pattern` / `table-pattern`](./table-selector.md) 对匹配上该规则的上游 MySQL/MariaDB 实例的表同步到下游的 `tagrt-schema`.`target-table`


### 使用示例


#### 分库分表合并

假设存在分库分表场景 - 将上游两个 MySQL 实例 `test_{1,2,3...}`.`t_{1,2,3...}` 同步到下游 TiDB 的 `test`.`t`

- 同步到下游的 `test`.`t`，则需要创建两个路由规则
  - `rule-1` 用来同步匹配上 `schema-pattern: "test_*"` 和 `table-pattern: "t_*"` 的表的 DMLs/DDLs 到下游的 `test`.`t` 
  - `rule-2` 用来同步匹配上 `schema-pattern: "test_*"` 的库的 DDLs （`create/drop schema xx`）
  - 如果下游 TiDB `schema: test` 已经存在， 并且不会被删除则可以省略 `rule-2`
  - 如果下游 TiDB `schema: test` 不存在，只设置了 `rule_1` 则同步会报错 `schema test doesn't exist`
```yaml
  rule-1:
    schema-pattern: "test_*"
    table-pattern: "t_*"
    target-schema: "test"
    target-table: "t"
  rule-2:
    schema-pattern: "test_*"
    target-schema: "test"
    
```
***

#### 分库合并

假设存在分库场景 - 将上游两个 MySQL 实例 `test_{1,2,3...}`.`t_{1,2,3...}` 同步到下游 TiDB 的 `test`.`t_{1,2,3...}`

- 同步到下游的 `test`.`t_{1,2,3...}`，创建一个路由规则即可
```yaml
  rule-1:
    schema-pattern: "test_*"
    target-schema: "test"
```

***

#### 错误的路由

假设存在下面两个路由规则，`test_1_bak`.`t_1_bak` 可以匹配上 `rule-1` 和 `rule-2`, 违反 table 路由的限制而报错

```yaml
  rule-1:
    schema-pattern: "test_*"
    table-pattern: "t_*"
    target-schema: "test"
    target-table: "t"
  rule-2:
    schema-pattern: "test_1_bak"
    table-pattern: "t_1_bak"
    target-schema: "test"
    target-table: "t_bak"
```



