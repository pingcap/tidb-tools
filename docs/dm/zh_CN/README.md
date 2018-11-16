DM (Data Migration) 用户文档
===

如果是第一次使用 DM，建议从 [Get Started](./get-started.md) 开始。

如果想更详细地了解 DM 的使用、限制甚至是原理，可以参考以下 index 直接阅读感兴趣的部分。

### DM 用户文档索引

#### 用户指引

[Get Started](./get-started.md)
[syncer/loader 升级](./upgrade-to-dm)
[Troubleshoot](./trouble-shoot.md)

#### 使用手册

[用户使用手册](./user-manual.md)
[使用限制](./restrictions.md)
[运维管理](./maintenance)
    - [DM Ansible 运维手册](./maintenance/dm-ansible.md)
    - [扩充/缩减 DM 集群](./maintenance/scale-out.md)
    - [部署目录结构](./maintenance/directory-structure.md)
    - [重启 DM 组件注意事项](./maintenance/caution-for-restart-dm.md)
    - [使用 dmctl 执行主-从切换](./maintenance/master-slave-switch.md)
    - [DM 监控与告警](./maintenance/metrics-alert.md)
[配置文件](./configuration)
    - [Task 配置文件介绍](./configuration/configuration.md)
    - [Task 配置项介绍](./configuration/argument-explanation.md)
    - [task.yaml 示例](./configuration/task.yaml)
[任务管理](./task-handling)
    - [dmctl 使用手册](./task-handling/dmctl-manual.md)
    - [任务管理命令](./task-handling/task-commands.md)
    - [上游数据库实例检查](./task-handling/check-mysql.md)
[分库分表](./shard-table)
    - [分库分表支持方案](./shard-table/merge-solution.md)
    - [sharding DDL 使用限制](./shard-table/restrictions.md)
    - [手动处理 sharding DDL lock](./shard-table/handle-DDL-lock.md)
- [错误处理](./exception-handling)
    - [skip 或 replace 异常 SQL](./exception-handling/skip-replace-sqls.md)
    - [重置数据同步任务](./exception-handling/reset-task.md)
[数据流过程](./data-interaction-details.md)
[FAQ](./FAQ.md)
[Change Log](./change-log.md)