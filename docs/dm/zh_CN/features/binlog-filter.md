binlog filter rule
===

#### 功能介绍

比 schema / table 同步黑白名单更加细粒度的过滤规则，可以指定只同步或者过滤掉某些 `schema / table` 的指定类型 binlog， 比如 `INSERT`，`TRUNCATE TABLE`

#### 参数配置

```
filters:
  rule-1:
    schema-pattern: "test_*"
    ​table-pattern: "t_*"
    ​events: ["truncate table", "drop table"]
    sql-pattern: ["^DROP\\s+PROCEDURE", "^CREATE\\s+PROCEDURE"]
    ​action: Ignore
```

#### 参数解释

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