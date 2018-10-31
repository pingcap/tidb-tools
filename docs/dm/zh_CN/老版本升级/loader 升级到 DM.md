loader 升级到 DM
===

loader 用于执行全量数据导入，在 DM 中对应于 `task-mode` 为 `full` 的同步任务。

但不同于 loader 的在导入前需要单独使用 mydumper 进行导出，在 DM 中的执行 `task-mode` 为 `full` 的 task 时，会自动先使用 mydumper 进行导出，然后再使用 load unit （对应于原 loader）进行导入。

从 loader 升级到 DM 时，主要变化包括部署与启动方式、同步任务执行方式、部分配置项的增减或修改等。

### 部署与启动

原 loader 通常以 `nohup` 等形式启动并在后台执行。在 DM 中则将进程的部署启动与任务执行进行了分离，使用 ansible 来部署并启动各组件，参见 [DM Ansible 运维手册]

当需要同时同步多个数据库实例的 mydumper 输出时，原 loader 通过手动独立地启动多个 loader 实例分别进行导入。在 DM 中可通过 ansible 的配置文件填写多个 dm-worker 实例信息直接部署多个 dm-worker。

### 执行同步任务

原 loader 通过在启动进程时以命令行参数或指定配置文件的形式在进程启动成功后即自动开始执行数据同步。在 DM 中则通过先在配置文件中定义数据同步任务，然后使用 dmctl 命令行工具对任务进行启停等管理操作，有关 dmctl 的任务管理功能，参见 [任务管理/dmctl 使用手册]

另外，原 loader 单个实例只能执行一个同步任务，原任务同步完成后，需要重启 loader 以执行新的同步任务。在 DM 中，dm-worker 长驻后台运行，当需要新增任务时，只需要使用 dmctl 并指定新的配置文件执行启动任务的操作即可，多个同步任务可同时并发进行。

即原 loader 与同步任务的关系为 一对一，dm-worker 与同步任务的关系为 一对多。

### 配置变化

原 loader 使用 TOML 格式的文件来定义进程的相关运行参数与同步任务参数。在 DM 中，进程参数仍可使用 TOML 格式文件指定（但更推荐使用 Andible 来部署启动），同步任务参数则通过使用 YAML 格式来指定，且当前支持两种不同的配置文件类型，参见 [配置文件/Task 配置文件介绍]与  [配置文件/Task 简易配置文件]，这两种格式的配置文件可通过 dmctl 进行双向转换。

以 [配置文件/Task 配置文件介绍] 文件类型为例，下面列举原 loader 与 DM 任务同步配置项间的对应关系如下

```
pool-size    		=>	loader / pool-size
dir          		=>	loader / dir
db           		=>	target-database
checkpoint-schema	=>	dm_meta	 # 下游存储的 meta 信息的 schema，table 为 {$task_name}_loader_checkpoint
alternative-db   		 # 已废弃
source-db      			 # 已废弃
route-rules   		=>	route-rules		 # pattern-schema / pattern-table 已分别更名为 schema-pattern / table-pattern
do-db			=>	black-white-list / do-dbs, do-tables  # DB, Table 的 filter 功能已重构，具体配置参见 task 配置文件
do-table			=>	black-white-list / do-dbs, do-tables  # DB, Table 的 filter 功能已重构，具体配置参见 task 配置文件
ignore-db			=>	black-white-list / ingore-dbs		  # DB, Table 的 filter 功能已重构，具体配置参见 task 配置文件
ignore-table			=>	black-white-list / ignore-tables	  # DB, Table 的 filter 功能已重构，具体配置参见 task 配置文件
rm-checkpoint			 # 已废弃，但有功能近似的配置项 remove-previous-checkpoint
```

以 [配置文件/Task 配置文件介绍] 文件类型为例，mydumper 与 DM 配置项间的对应关系如下

```
host, port, user, password		# 无直接对应参数，dm-worker 部署时即已确定
threads			=>	mydumper / threads
chunk-filesize			=>	mydumper / chunk-filesize
skip-tz-utc			=>	mydumper / skip-tz-utc  # 其它参数统一通过 mydumper / extra-args 指定，形式与 mydumper 中使用时一致
```
