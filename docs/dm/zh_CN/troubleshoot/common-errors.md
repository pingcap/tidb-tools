常见错误解决方案
===

1. query-task 或者查看 log 发现 `Access denied for user 'root'@'172.31.43.27' (using password: YES)`

   在 DM 的所有相关配置文件中，数据库相关的密码需要使用 dmctl 加密后的密文（如果数据库密码为空，则不需要加密）。 

   了解如何使用 dmctl 加密明文密码，可以参考 [dmctl 加密上游 MySQL 用户密码](../maintenance/dm-ansible.md#dmctl-加密上游-mysql-用户密码)。

   此外，DM 在运行过程中，相关的上下游数据库用户需要具备相应的读写权限。DM 在启动任务过程中，也会自动进行部分权限检查，具体见 [上游 MySQL 实例权限](../task-handling/check-mysql.md)

2. 不兼容 DDL 处理

遇到下面的错误的时候，需要使用 dmctl 手动处理该错误（包括 跳过该 DDL 或 使用用户指定的 DDL 替代原 DDL），具体操作方式参见 [skip 或 replace 异常 SQL](./skip-replace-sqls.md)
```sql
encountered incompatible DDL in TiDB: %s
	please confirm your DDL statement is correct and needed.
	for TiDB compatible DDL, please see the docs:
	  English version: https://github.com/pingcap/docs/blob/master/sql/ddl.md
	  Chinese version: https://github.com/pingcap/docs-cn/blob/master/sql/ddl.md
	if the DDL is not needed, you can use dm-ctl to skip it, otherwise u also can use dm-ctl to replace it.
```

并且请关注 TiDB 当前并不兼容 MySQL 支持的所有 DDL，已支持的 DDL 信息可参见: <https://github.com/pingcap/docs-cn/blob/master/sql/ddl.md>

