常见错误解决方案
===

1. query-task 或者查看 log 发现 `Access denied for user 'root'@'172.31.43.27' (using password: YES)`

在 DM 的所有相关配置文件中，数据库相关的密码需要使用 dmctl 加密后的密文（如果数据库密码为空，则不需要加密）。 

了解如何使用 dmctl 加密明文密码，可以参考 [dmctl 加密上游 MySQL 用户密码](./maintenance/dm-ansible.md#dmctl-加密上游-mysql-用户密码)。

此外，DM 在运行过程中，相关的上下游数据库用户需要具备相应的读写权限。DM 在启动任务过程中，也会自动进行部分权限检查，具体见 [上游 MySQL 实例权限](./task-handling/check-mysql.md)。
 
