DM (Data Migration) 用户文档
===

如果是第一次使用 DM，建议从 [Get Started](./get-started.md) 开始。

如果想更详细地了解 DM 的使用、限制甚至是原理，可以参考以下 index 直接阅读感兴趣的部分。


### 简介

DM (Data Migration) 是一体化数据同步任务管理平台，支持全量备份和 MariaDB/MySQL binlog 增量同步，设计的主要目的是
   - 标准化 （例如:工具运行、错误定义）
   - 降低运维使用成本
   - 简化错误处理流程
   - 提升产品使用体验

### 用户指引

- [Get Started](./get-started.md)
- [从 syncer/loader 升级](./upgrade-to-dm)
- [Troubleshoot](./troubleshoot)

### 使用手册

- [DM 介绍](./overview.md)
- [使用限制](./restrictions.md)
- [同步前置检查](./precheck.md)
- [同步功能介绍](./features)
    - [table 路由](./features/table-route.md)
    - [table 黑白名单](./features/black-white-list.md)
    - [binlog 过滤](./features/binlog-filter.md)
    - [列值转换](./features/column-mapping.md)
    - [同步延迟监控](./features/heartbeat.md)
    - [relay-log](./features/relay-log.md)
- [运维管理](./maintenance)
    - [DM Ansible 运维手册](./maintenance/dm-ansible.md)
    - [扩充/缩减 DM 集群](./maintenance/scale-out.md)
    - [部署目录结构](./maintenance/directory-structure.md)
    - [重启 DM 组件注意事项](./maintenance/caution-for-restart-dm.md)
    - [使用 dmctl 执行主-从切换](./maintenance/master-slave-switch.md)
    - [query-status内容介绍](./maintenance/query-status.md)
    - [DM 监控与告警](./maintenance/metrics-alert.md)
- [配置文件](./configuration)
    - [Task 配置文件介绍](./configuration/configuration.md)
    - [task.yaml 示例](./configuration/task.yaml)
- [任务管理](./task-handling)
    - [dmctl 使用手册](./task-handling/dmctl-manual.md)
    - [任务管理命令](./task-handling/task-commands.md)
    - [上游数据库实例检查](./task-handling/check-mysql.md)
- [分库分表](./shard-table)
    - [分库分表支持方案](./shard-table/merge-solution.md)
    - [sharding DDL 使用限制](./shard-table/restrictions.md)
    - [手动处理 sharding DDL lock](./shard-table/handle-DDL-lock.md)
- [错误处理](./troubleshoot)
    - [skip 或 replace 异常 SQL](./troubleshoot/skip-replace-sqls.md)
    - [重置数据同步任务](./troubleshoot/reset-task.md)
- [数据流过程](./data-interaction-details.md)
- [FAQ](./FAQ.md)
- [Change Log](./change-log.md)
