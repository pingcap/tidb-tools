同步表黑白名单
===

### 索引
- [功能介绍](#功能介绍)
- [参数配置](#参数配置)
- [参数解释](#参数解释)
- [规则介绍](#规则介绍)
- [使用示例](#使用示例)

### 功能介绍

上游数据库实例表的黑白名单过滤规则。过滤规则类似于 MySQL replication-rules-db / tables, 可以用来过滤或者只同步某些 database 或者某些 table 的所有操作

### 参数配置

```yaml
black-white-list:
  rule-1:						
    do-dbs: ["~^test.*"]         # 以 ~ 字符开头，表示规则是正则表达式
    ignore-dbs: ["mysql"]
    do-tables:
    - db-name: "~^test.*"
      tbl-name: "~^t.*"
    - db-name: "test"
      tbl-name: "t"
    ignore-tables:
    - db-name: "test"
      tbl-name: "log"
```

### 参数解释

- `do-dbs` 要同步的库的白名单
- `ignore-dbs` 要同步的库的黑名单
- `do-tables` 要同步的表的白名单
- `ignore-tables` 要同步的表的黑名单
- 上面黑白名单中 以 ~ 字符开头名称为[正则表达式](https://golang.org/pkg/regexp/syntax/#hdr-Syntax)
 
### 规则介绍

判断 table `test`.`t` 是否被过滤

1. 首先 *schema 过滤判断*
    1. 如果 `do-dbs` 不为空
       1. 判断 `do-dbs` 中有没有匹配的 schema，没有则过滤 `test`.`t`
       2. 否则进入 *table 过滤判断*
    2. 如果 `do-dbs` 为空，`ignore-dbs` 不为空
       1. 判断 `ignore-dbs` 里面有没有匹配的 schema，如果有则过滤 `test`.`t`
       2. 否则进入 *table 过滤判断*
    3. 如果 `do-dbs` 和 `ignore-dbs` 都为空，则进入 *table 过滤判断*
2. 然后 *table 过滤判断*
    1. 如果 `do-tables` 不为空
       1. 判断 `do-tables` 中有没有匹配的 table, 如果有则同步 `test`.`t`
       2. 否则过滤 `test`.`t`
    2. 如果 `ignore-tables` 不为空
       1. 判断 `ignore-tables` 中有没有匹配的 table, 如果有则过滤 `test`.`t`
       2. 否则同步 `test`.`t`
     3. 如果 `do-tables` 和 `ignore-tables` 都为空，则同步 `test`.`t`

注意： 判断 schema `test` 是否被过滤，只进行 *schema 过滤判断*

### 使用示例

假设上游 MySQL 实例包含下面的 tables:

```
`logs`.`messages_2016`
`logs`.`messages_2017`
`logs`.`messages_2018`
`forum`.`users`
`forum`.`messages`
`forum_backup_2016`.`messages`
`forum_backup_2017`.`messages`
`forum_backup_2018`.`messages`
```

配置如下:

```yaml
black-white-list:
  bw-rule:
    do-dbs: ["forum_backup_2018", "forum"]
    ignore-dbs: ["~^forum_backup_"]
    do-tables:
    - db-name: "logs"
      tbl-name: "~_2018$"
    - db-name: "~^forum.*"
​      tbl-name: "messages"
    ignore-tables:
    - db-name: "~.*"
​      tbl-name: "^messages.*"
```

应用 `bw-rule` 规则后

| table | 是否过滤| 过滤的原因 |
|----:|:----|:--------------|
| `logs`.`messages_2016` | 是 | schema `logs` 没有匹配到 `do-dbs` 任意一项 |
| `logs`.`messages_2017` | 是 | schema `logs` 没有匹配到 `do-dbs` 任意一项 |
| `logs`.`messages_2018` | 是 | schema `logs` 没有匹配到 `do-dbs` 任意一项 |
| `forum_backup_2016`.`messages` | 是 | schema `forum_backup_2016` 没有匹配到 `do-dbs` 任意一项 |
| `forum_backup_2017`.`messages` | 是 | schema `forum_backup_2017` 没有匹配到 `do-dbs` 任意一项 |
| `forum`.`users` | 是 | 1. schema `forum` 匹配到 `do-dbs` 进入 table 过滤<br> 2. schema 和 table 没有匹配到 `do-tables` 和 `ignore-tables` 中任意一项，并且 `do-tables` 不为空，因此过滤 |
| `forum`.`messages` | 否 | 1. schema `forum` 匹配到 `do-dbs` 进入 table 过滤<br> 2. table `messages` 在 `do-tables` 的 `db-name: "~^forum.*",tbl-name: "messages"` |
| `forum_backup_2018`.`messages` | 否 | 1. schema `forum_backup_2018` 匹配到 `do-dbs` 进入 table 过滤<br> 2. schema 和 table 匹配到 `do-tables` 的  `db-name: "~^forum.*",tbl-name: "messages"` |
