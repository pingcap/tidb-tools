loader 升级到 DM
===

loader 用于导入 [MyDumper](https://github.com/pingcap/docs-cn/blob/master/tools/mydumper.md) 导出的全量数据。

在 DM 中的执行 `task-mode` 为 `full` 的 task 时，会自动使用 dumper 处理单元导出数据，然后使用 loader 处理单元进行导入。

从 loader 升级到 DM 时，只需要生成对应的 task 配置文件，然后通过 dmctl  运行任务即，可参见 [dmctl 使用手册](../task-handling/dmctl-manual.md)。

### 配置变化

原 loader 使用 TOML 格式的文件来定义进程的相关运行参数与同步任务参数，在 DM 中 Task 配置文件参数则通过使用 YAML 格式来指定。

以 [Task 配置文件介绍](../configuration/configuration.md) 文件类型为例，下面列举原 loader 与 DM 任务同步配置项间的对应关系如下

```
pool-size                   =>	loader / pool-size
dir                         =>	loader / dir
db                          =>	target-database
alternative-db              # 已废弃
source-db                   # 已废弃
route-rules                 =>	route-rules		 # pattern-schema / pattern-table 已分别更名为 schema-pattern / table-pattern
do-db                       =>	black-white-list / do-dbs, do-tables  # DB, Table 的 filter 功能已重构，具体配置参见 task 配置文件
do-table                    =>	black-white-list / do-dbs, do-tables  # DB, Table 的 filter 功能已重构，具体配置参见 task 配置文件
ignore-db                   =>	black-white-list / ingore-dbs		  # DB, Table 的 filter 功能已重构，具体配置参见 task 配置文件
ignore-table                =>	black-white-list / ignore-tables	  # DB, Table 的 filter 功能已重构，具体配置参见 task 配置文件
rm-checkpoint               # 已废弃，但有功能近似的配置项 remove-previous-checkpoint
```

以 [Task 配置文件介绍](../configuration/configuration.md) 文件类型为例，mydumper 与 DM 配置项间的对应关系如下

```
host, port, user, password      # 无直接对应参数，DM-worker 部署时即已确定
threads                         =>	mydumper / threads
chunk-filesize                  =>	mydumper / chunk-filesize
skip-tz-utc                     =>	mydumper / skip-tz-utc  # 其它参数统一通过 mydumper / extra-args 指定，形式与 mydumper 中使用时一致
```
