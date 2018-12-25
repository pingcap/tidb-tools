black whitle list
===

#### 功能介绍

上游数据库实例表的黑白名单过滤规则。过滤规则类似于 MySQL replication-rules-db / tables, 可以用来过滤或者只同步某些 database 或者某些 table 的所有操作

#### 参数配置

```
black-white-list:
  rule-1:						
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

#### 参数解释

- `do-dbs` 要同步的库的白名单
- `ignore-dbs` 要同步的库的黑名单
- `do-tables` 要同步的表的白名单
- `ignore-tables` 要同步的表的黑名单
- 上面黑白名单中# 以 ~ 字符开头名称代表，改名称将应用与正则表达式匹配
 
#### 规则介绍
1. 首先 schema 过滤
    1. 如果 `do-dbs` 不为空，则判断 `do-dbs` 中有没有匹配的 schema，没有没有则返回并且忽略，否则进入 table 过滤
    2. 如果 `do-dbs` 为空，`ignore-dbs` 不为空，则判断 `ignore-dbs` 里面有没有匹配的 schema，如果有则返回并且忽略，否则进入 table 过滤
    3. 如果 `do-dbs` 和 `ignore-dbs` 都为空，则进入 table 过滤
2. 然后 table 过滤
    1. 如果 `do-tables` 不为空，则判断 `do-tables` 中有没有匹配的 rule, 如果有则返回并且执行，否则进入 table 过滤的步骤 2
    2. 如果 `ignore tables` 不为空，则判断 `ignore-tables` 中有没有匹配的 rule, 如果有则返回并且忽略， 否则进入 table 过滤的步骤 3
    3. 如果 `do-tables` 不为空，则返回并且忽略，否则返回并且执行