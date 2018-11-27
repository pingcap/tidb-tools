syncer 升级到 DM
===

syncer 用于执行全量数据导入，在 DM 中对应于 `task-mode` 为 `incremental` 的同步任务。在 DM 中，与原 syncer 功能对应的组件为 dm-worker 内的 syncer 处理单元

从 syncer 升级到 DM 时，只需要生成对应的 task 配置文件，然后通过 dmctl  运行任务即，可参见 [dmctl 使用手册](../task-handling/dmctl-manual.md)。


## 配置变化

原 syncer 使用 TOML 格式的文件来定义进程的相关运行参数与同步任务参数，在 DM 中 Task 配置文件参数则通过使用 YAML 格式来指定。

以 [Task 配置文件介绍](../configuration/configuration.md) 文件类型为例，下面列举原 syncer 与 DM 任务同步配置项间的对应关系如下

```
server-id                       # 已转移到 DM-worker 配置中
flavor                          # 已转移到 DM-worker 配置中
enable-gtid                     # 已转移到 DM-worker 配置中
auto-fix-gtid                   # 已转移到 DM-worker 配置中
meta                            =>	mysql-instances / meta	 # Task 的 instance 配置项 meta， 直接指定 binlog-name, binlog-pos
persistent-dir                  # 已废弃
worker-count                    =>	syncer / worker-count
batch                           =>	syncer / batch
max-retry                       =>	syncer / max-retry
do-db                           =>	black-white-list / do-dbs, do-tables  # DB, Table 的 filter 功能已重构，具体配置参见 task 配置文件
do-table                        =>	black-white-list / do-dbs, do-tables  # DB, Table 的 filter 功能已重构，具体配置参见 task 配置文件
ignore-db                       =>	black-white-list / ingore-dbs	      # DB, Table 的 filter 功能已重构，具体配置参见 task 配置文件
ignore-table                    =>	black-white-list / ignore-tables	  # DB, Table 的 filter 功能已重构，具体配置参见 task 配置文件
skip-ddls                       # 已废弃，请使用 filters
skip-sqls                       # 已废弃，请使用 filters
skip-events                     # 已废弃，请使用 filters
skip-dmls                       # 已废弃，请使用 filters
route-rules                     =>	route-rules                           # pattern-schema / pattern-table 已分别更名为 schema-pattern / table-pattern
from                            =>	mysql-instances / config              # Task 的 instance 配置项 config， 应与 DM-worker 部署时的上游 MySQL 信息一致
to                              =>	target-database
disable-detect                  =>	syncer / disable-detect
safe-mode                       =>	syncer / safe-mode
stop-on-ddl                     # 已废弃
execute-ddl-timeout             # 暂不支持
execute-dml-timeout             # 暂不支持
```
