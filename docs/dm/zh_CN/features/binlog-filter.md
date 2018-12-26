binlog 过滤
===

### 索引
- [功能介绍](#功能介绍)
- [参数配置](#参数配置)
- [参数解释](#参数解释)
- [使用示例](#使用示例)
  - [过滤分库分表的所有删除操作](#过滤分库分表的所有删除操作)
  - [只同步分库分表的 DML 操作](#只同步分库分表的-DML-操作)
  - [过滤 TiDB 不支持的 SQL 的语句](#过滤-TiDB-不支持的-SQL-的语句)

### 功能介绍

比 schema / table 同步黑白名单更加细粒度的过滤规则，可以指定只同步或者过滤掉某些 `schema / table` 的指定类型 binlog， 比如 `INSERT`，`TRUNCATE TABLE`

### 参数配置

```
filters:
  rule-1:
    schema-pattern: "test_*"
    ​table-pattern: "t_*"
    ​events: ["truncate table", "drop table"]
    sql-pattern: ["^DROP\\s+PROCEDURE", "^CREATE\\s+PROCEDURE"]
    ​action: Ignore
```

### 参数解释

- [`schema-pattern` / `table-pattern`](./table-selector.md): 对匹配上该规则的上游 MySQL/MariaDB 实例的表的 binlog events 或者 DDL SQLs 进行以下规则过滤
- events: binlog events 数组

| event           | 分类 | 解释                           |
| --------------- | ---- | ----------------------------- |
| all             |      | 代表包含下面所有的 events        |
| all dml         |      | 代表包含下面所有 DML events     |
| all ddl         |      | 代表包含下面所有 DML events     |
| none            |      | 代表不包含下面所有 events        |
| none ddl        |      | 代表不包含下面所有 DDL events    |
| none dml        |      | 代表不包含下面所有 DML events    |
| insert          | DML  | insert DML event              |
| update          | DML  | update DML event              |
| delete          | DML  | delete DML event              |
| create database | DDL  | create database event         |
| drop database   | DDL  | drop database event           |
| create table    | DDL  | create table event            |
| create index    | DDL  | create index event            |
| drop table      | DDL  | drop table event              |
| truncate table  | DDL  | truncate table event          |
| rename table    | DDL  | rename table event            |
| drop index      | DDL  | drop index event              |
| alter table     | DDL  | alter table event             |
- sql-pattern: 用于过滤指定的 DDL SQLs， 支持正则表达式匹配，例如上面示例 `"^DROP\\s+PROCEDURE"`。 注意： 如果 `sql-pattern` 为空，则不进行任何过滤
- action: string(`Do` / `Ignore`);  进行下面规则判断，满足其中之一则过滤，否则不过滤
    - Do: 白名单，不在该 rule 的 events 中，或者 sql-pattern 不为空的话，对应的 sql 也不在 sql-pattern 中
    - Ignore: 黑名单，在该 rule 的 events 或 sql-pattern 中

### 使用示例

下面例子都假设存在分库分表场景 - 将上游两个 MySQL 实例 `test_{1,2,3...}`.`t_{1,2,3...}` 同步到下游 TiDB 的 `test`.`t`.

#### 过滤分库分表的所有删除操作

需要设置下面两个 filter rules 来过滤掉所有的删除操作

```yaml
filters:
  filter-table-rule:
    schema-pattern: "test_*"
    table-pattern: "t_*"
    events: ["truncate table", "drop table", "delete"]
    action: Ignore
  filter-schema-rule:
    schema-pattern: "test_*"
    events: ["drop database"]
    action: Ignore
```

- `filter-table-rule` 过滤所有匹配到 pattern `test_*`.`t_*` 的 table 的 `turncate table`、`drop table`、`delete statement` 操作
- `filter-schema-rule` 过滤所有匹配到 pattern `test_*` 的 schema 的 `drop database` 操作

***

#### 只同步分库分表的 DML 操作

设置下面两个 filter rules 只同步 DML 操作

```yaml
filters:
  do-table-rule:
    schema-pattern: "test_*"
    table-pattern: "t_*"
    events: ["create table", "all dml"]
    action: Do
  do-schema-rule:
    schema-pattern: "test_*"
    events: ["create database"]
    action: Do
```

- `do-table-rule` 只同步所有匹配到 pattern `test_*`.`t_*` 的 table 的 `create table`、`insert`、`update`、`delete` 操作
- `do-schema-rule` 只同步所有匹配到 pattern `test_*` 的 schema 的 `create database` 操作

注意：同步 `create database/table` 的原因是创建库和表后才能同步 `DML`

***

#### 过滤 TiDB 不支持的 SQL 的语句

设置下面的规则过滤不支持的 `PROCEDURE statement`

```yaml
filters:
  filter-procedure-rule:
    schema-pattern: "test_*"
    table-pattern: "t_*"
    sql-pattern: ["^DROP\\s+PROCEDURE", "^CREATE\\s+PROCEDURE"]
    action: Ignore
```

- `filter-procedure-rule` 过滤所有匹配到 pattern `test_*`.`t_*` 的 table 的 `^CREATE\\s+PROCEDURE`、`^DROP\\s+PROCEDURE` 操作