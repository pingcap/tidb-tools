syncer 升级到 DM
===

syncer 用于执行全量数据导入，在 DM 中对应于 `task-mode` 为 `incremental` 的同步任务。在 DM 中，与原 syncer 功能对应的组件为 dm-worker 内的 sync unit。

从 syncer 升级到 DM 时，主要变化包括部署与启动方式、同步任务执行方式、部分配置项的增减或修改等。

## 部署与启动

原 syncer 通常以 `nohup` 等形式启动并在后台执行。在 DM 中则将进程的部署启动与任务执行进行了分离，使用 dm-ansible 来部署并启动各组件，参见 [DM Ansible 运维手册](../maintenance/dm-ansible.md)。

当需要同时同步多个数据库实例时，原 syncer 通过手动独立地启动多个 syncer 实例分别进行导入。在 DM 中可通过 dm-ansible 的配置文件填写多个 dm-worker 实例信息直接部署多个 dm-worker。

## 执行同步任务

原 syncer 通过在启动进程时以命令行参数或指定配置文件的形式在进程启动成功后即自动开始执行数据同步。在 DM 中则通过先在配置文件中定义数据同步任务，然后使用 dmctl 命令行工具对任务进行启停等管理操作，有关 dmctl 的任务管理功能，参见 [dmctl 使用手册](../task-handling/dmctl-manual.md)。

另外，原 syncer 单个实例只能执行一个同步任务，原任务同步完成后，需要重启 syncer 以执行新的同步任务。在 DM 中，dm-worker 长驻后台运行，当需要新增任务时，只需要使用 dmctl 并指定新的配置文件执行启动任务的操作即可，多个同步任务可同时并发进行。

即原 syncer 与同步任务的关系为 一对一，dm-worker 与同步任务的关系为 一对多。

## 配置变化

原 syncer 使用 TOML 格式的文件来定义进程的相关运行参数与同步任务参数。在 DM 中，进程参数仍可使用 TOML 格式文件指定（但更推荐使用 dm-ansible 来部署启动），同步任务参数则通过使用 YAML 格式来指定。

以 [Task 配置文件介绍](../configuration/configuration.md) 文件类型为例，下面列举原 syncer 与 DM 任务同步配置项间的对应关系如下

```
server-id                       # 已转移到 dm-worker 配置中
flavor                          # 已转移到 dm-worker 配置中
enable-gtid                     # 已转移到 dm-worker 配置中
auto-fix-gtid                   # 已转移到 dm-worker 配置中
meta                            =>	mysql-instances / meta	 # DM 中 meta 直接指定 binlog-name, binlog-pos，而不是指定 meta file name
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
from                            =>	mysql-instances / config              # 应与 dm-worker 部署时的上游 MySQL 信息一致
to                              =>	target-database
disable-detect                  =>	syncer / disable-detect
safe-mode                       =>	syncer / safe-mode
stop-on-ddl                     # 已废弃
execute-ddl-timeout             # 暂不支持
execute-dml-timeout             # 暂不支持
```
