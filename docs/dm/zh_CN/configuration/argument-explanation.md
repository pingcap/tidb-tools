Task 配置项介绍
===

### 任务模式 - task-mode

`task-mode`: string( `full` / `incremental` / `all` ); 默认为 `all`

解释：任务模式，可以通过任务模式来指定需要执行的数据迁移工作
- `full` - 只全量备份上游数据库，然后全量导入到下游数据库
- `incremental` - 只通过 binlog 把上游数据库的增量修改同步到下游数据库
- `all` - `full` + `incremental`，先全量备份上游数据库，导入到下游数据库，然后从全量数据备份时导出的位置信息（binlog position / GTID）开始通过 binlog 增量同步数据到下游数据库


### 路由规则 - route rule

```
# schema-pattern / table-pattern 采用 wildcard 的匹配规则
schema level:
​    schema-pattern: "test_*"
​    target-schema: "test"

table level:
​    schema-pattern: "test_*"
​    table-pattern: "t_*"
​    target-schema: "test"
​    target-table: "t"
```

解释：将上游匹配 `schema-pattern` / `table-pattern` 的表数据同步到下游 `target-schema` / `target-table`。可以设置 schema / table 两个级别的 route rule。

例如上面示例
- schema level - 将上游所有匹配 `test_*` 的 schema 的表都同步到下游 test 的 schema 中  
    比如 `schema: test_1 - tables [a, b, c]`  =>  `schema:test -  tables [a, b, c]`
- table level - 将上游 schema 匹配 `test_*` 且 table 匹配 `t_*` 的表都同步到下游 `schema:test table:t` 的表中

注意
- table level rule 优先级高于 schema level 优先级
- 同一 level 的不能存在超过一个配置规则


### 同步表的黑白名单 - black white list rule

```
instance:						
​    do-dbs: ["~^test.*", "do"]         # 以 ~ 字符开头，表示规则是正则表达式
​    ignore-dbs: ["mysql", "ignored"]
​    do-tables:
​    - db-name: "~^test.*"
​      tbl-name: "~^t.*"
​    - db-name: "do"
​      tbl-name: "do"
​    ignore-tables:
​    - db-name: "do"
​      tbl-name: "do"
```

解释: 上游数据库实例表的黑白名单过滤规则。过滤规则类似于 MySQL replication-rules-db / tables, 下面简单介绍一下过滤流程
1. 首先进行 schema level 级别过滤
    1. 如果 `do-dbs` 不为空，则判断 `do-dbs` 中有没有匹配的 schema，如果有则进入 table level 级别过滤，否则返回并且忽略
    2. 如果 `do-dbs` 为空，`ignore-dbs` 不为空，则判断 `ignore-dbs` 里面有没有匹配的 schema，如果有则返回并且忽略，否则进入 table level 级别进行过滤
    3. 如果 `do-dbs` 和 `ignore-dbs` 都为空，则进入 table level 级别进行过滤
2. 然后进行 table level 级别过滤
    1. 如果 `do-tables` 不为空，则判断 `do-tables` 中有没有匹配的 rule, 如果有则返回并且执行
    2. 如果 `ignore tables` 不为空，则判断 `ignore-tables` 中有没有匹配的 rule, 如果有则返回并且忽略
    3. 如果 `do-tables` 不为空，则返回并且忽略，否则返回并且执行


### binlog event 过滤规则 - filter-rule

(匹配规则看 action 解释)

```
# table level
user-filter-1:
​    schema-pattern: "test_*"     # schema-pattern / table-pattern 采用 wildcard 的匹配规则
​    table-pattern: "t_*"
​    events: ["truncate table", "drop table"]
​    sql-pattern: ["^DROP\\s+PROCEDURE", "^CREATE\\s+PROCEDURE"]
​    action: Ignore

# schema level
user-filter-2:
​    schema-pattern: "test_*"
​    events: ["All DML"]
​    action: Do
```

解释: 设置与 `schema-pattern` / `table-pattern` 的匹配的上游表的 binlog events 和 DDL SQL 的黑白名单过滤规则
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

- sql-pattern: 可以用于过滤特定具体的 DDL SQL，匹配规则支持正则表达式，例如上面示例 `"^DROP\\s+PROCEDURE"`。 注意： 如果 `sql-pattern` 为空，则不进行任何过滤（过滤规则见 action）
- action: string(`Do` / `Ignore`);  对根据 `schema-pattern` / `table-pattern` 匹配到的 rules，进行下面判断，满足其中之一则过滤。 如果没有任何 rule 满足，则执行
    - Do: 白名单，不在该 rule 的 events 及 sql-pattern (不为空) 中
    - Ignore: 黑名单，在该 rule 的 events 或 sql-pattern 中


### column 修改映射 - column mapping rule

```
instance-1:
​    schema-pattern: "test_*"    # schema-pattern / table-pattern 采用 wildcard 的匹配规则
​    table-pattern: "t_*"
​    expression: "partition id"
​    source-column: "id"
​    target-column: "id"
​    arguments: ["1", "test_", "t_"]
instance-2:
​    schema-pattern: "test_*"
​    table-pattern: "t_*"
​    expression: "partition id"
​    source-column: "id"
​    target-column: "id"
​    arguments: ["2", "test_", "t_"]
```

解释: 上游数据库实例与 `schema-pattern` / `table-pattern` 匹配的表的 column 映射修改规则，可用于分库分表的自增主键的冲突合并。

- source-column, target-column: 把 `source-column` 的数据通过 `expression` 的计算覆盖到 `target-column` 的数据
- expression: 对 column 数据进行转换的表达式，目前只支持下面内置计算表达式
    - `partition id`
        需要用户设置 `arguments` 为 `[instance_id, prefix of schema, prefix of table]`。如果 `instance_id` / `prefix of schema` / `prefix of table` 任意为空，则其对应的值不参与下面的 `partition id` 运算 .
        ![partition ID](./partition-id.png)
        限制如下：
        - 只能适用于 bigint 的数据列
        - origin ID value 满足 (>= 0, <= 17592186044415)（默认 44 bits）
