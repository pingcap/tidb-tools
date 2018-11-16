DM 监控与告警
===

目前只有 dm-worker 提供了 metrics， dm-master 暂未提供。

### 内容索引
- [任务监控项](#Task)
- [Relay Log Unit 监控项](#Relay-Log-Unit)
- [Mydumper Unit 监控项](#Mydumper-Unit)
- [Load Unit 监控项](#Load-Unit)
- [Binlog-Replication-Unit 监控项](#Binlog-Replication-Unit)


### Task

#### task state

query: `dm_worker_task_state{task="$task",instance="$instance"}`

说明：同步子任务的状态

告警：当子任务状态处于 paused 超过 10 分钟时


### Relay Log Unit

DM 中的 relay log 的原理和功能与 MySQL slave relay log 类似，参见 <https://dev.mysql.com/doc/refman/5.7/en/slave-logs-relaylog.html>。

#### relay log data corruption

query: `changes(dm_relay_data_corruption{instance="$instance"}[1m])`

说明：对应 instance 上面 relay log 文件损坏的个数。

告警：需要告警

#### fail to read binlog from master

query: `changes(dm_relay_read_error_count{instance="$instance"}[1m])`

说明：relay 模块从上游的 mysql 读取 binlog 时遇到的错误数。

告警：需要告警

#### fail to write relay log

query: `changes(dm_relay_write_error_count{instance="$instance"}[1m])`

说明：relay 模块写 binlog 到磁盘时遇到的错误数。

告警：需要告警

#### storage remain

query: `dm_relay_space{instance="$instance", type="available"}`

说明：relay log 组件占有的磁盘的剩余可用容量。

告警：当小于 10G 的时候需要告警

#### process exits with error

query: `changes(dm_relay_exit_with_error_count{instance="$instance"}[1m])`

说明：relay log 模块 在 dm-worker 内部遇到错误并且退出了。

告警：需要告警

#### binlog file gap between master and relay

query: `dm_relay_binlog_file{instance="$instance", node="master"} - ON(instance, job) dm_relay_binlog_file{instance="$instance", node="relay"}`

说明：relay 与上游 master 相比落后的 binlog file 个数。

告警：当落后 binlog file 个数超过 1 个（不含 1 个）且持续 10 分钟时

#### storage capacity

query: `dm_relay_space{instance="$instance", type="capacity"}`

说明：relay log 组件占有的磁盘的总容量。

#### binlog file index

query: `dm_relay_binlog_file{instance="$instance"}`

说明：relay log 最新的文件序列号。如 value = 1 表示 relay-log.000001，也代表已经从上游 MySQL 拉取的最大 binlog 文件序号。

#### binlog pos 

query: `dm_relay_binlog_pos{instance="$instance"}`

说明：relay log 的 position 移动情况。由于文件会 rotate，所以会出现数字骤减重新增长的情况。

#### read binlog duration

query: `histogram_quantile(0.90, sum(rate(dm_relay_read_binlog_duration_bucket{instance="$instance"}[1m])) by (le))`

说明：relay 模块从上游的 MySQL 读取 binlog 的时延，单位：秒。

#### write relay log duration

query: `histogram_quantile(0.90, sum(rate(dm_relay_write_duration_bucket{instance="$instance"}[1m])) by (le))`

说明：relay 模块每次写 binlog 到磁盘的时延，单位: 秒。

#### binlog size

query: `histogram_quantile(0.90, sum(rate(dm_relay_write_size_bucket{instance="$instance"}[1m])) by (le))`

说明：relay 模块写到磁盘的单条 binlog 的大小。


### load dump files

该 metric 仅在 `task-mode` 为 `full` 或者 `all` 模式下会有值。

#### load progress

query: `dm_loader_progress{task="$task",instance="$instance"}`

说明：loader 导入过程的进度百分比，值变化范围为：0 %- 100 %。

#### data file size

query: `dm_loader_data_size_count{task="$task",instance="$instance"}`

说明：loader 导入的全量数据中数据文件（内含 `INSERT INTO` 语句）的总大小。


### Mydumper Unit

#### dump process exits with error

query: `changes(dm_mydumper_exit_with_error_count{task="$task",instance="$instance"}[1m])`

说明：mydumper 模块 在 dm-worker 内部遇到错误并且退出了。

告警：需要告警


### load Unit

#### load process exits with error

query: `changes(dm_loader_exit_with_error_count{task="$task",instance="$instance"}[1m])`

说明：loader 模块在 dm-worker 内部遇到错误并且退出了。 

告警：需要告警

#### table count

query: `dm_loader_table_count{task="$task",instance="$instance"}`

说明：loader 导入的全量数据中 table 的数量总和。

#### data file count

query: `dm_loader_data_file_count{task="$task",instance="$instance"}`

说明：loader 导入的全量数据中数据文件（内含 `INSERT INTO` 语句）的数量总和。

#### latency of execute transaction

query: `histogram_quantile(0.90, sum(rate(dm_loader_txn_duration_time_bucket{task="$task", instance="$instance"}[1m])) by (le))`

说明：loader 在执行事务的时延，单位：秒。  

#### latency of query

query: `histogram_quantile(0.90, sum(rate(dm_loader_query_duration_time_bucket{task="$task", instance="$instance"}[1m])) by (le))`

说明：loader 执行 query 的耗时，单位：秒。


### Binlog Replication Unit

#### process exist with error

query: `changes(dm_syncer_exit_with_error_count{task="$task", instance="$instance"}[1m])`

说明：syncer 模块 在 dm-worker 内部遇到错误并且退出了。

告警：需要告警

#### binlog file gap between master and syncer

query：`dm_syncer_binlog_file{instance="$instance", task="$task", node="master"} - ON(instance, task, job) dm_syncer_binlog_file{instance="$instance", task="$task", node="syncer"}`

说明：syncer 与上游 master 相比落后的 binlog file 个数。

告警：当落后 binlog file 个数超过 1 个（不含 1 个）且持续 10 分钟时

#### binlog file gap between relay and syncer

query：`dm_relay_binlog_file{instance="$instance", node="relay"} - ON(instance, job) dm_syncer_binlog_file{instance="$instance", task="$task", node="syncer"}`

说明：syncer 与 relay 相比落后的 binlog file 个数。

告警：当落后 binlog file 个数超过 1 个（不含 1 个）且持续 10 分钟时

#### remaining time to sync

query: `dm_syncer_remaining_time{task="$task", instance="$instance"}`

说明：预计 syncer 还需要多少分钟可以和 master 完全同步，单位: 分钟。

#### replicate lag

query: `dm_syncer_replication_lag{task="$task", instance="$instance"}`

说明：master 到 syncer 的 binlog 复制延迟时间，单位：秒。

#### binlog event qps 

query: `rate(dm_syncer_binlog_transform_cost_count{task="$task", instance="$instance"}[1m])`

说明：单位时间内接收到的 binlog event 数量 (不包含需要跳过的 event)。

#### skipped binlog event qps 

query: `rate(dm_syncer_binlog_skipped_events_total{task="$task", instance="$instance"}[1m])`

说明：单位时间内接收到的需要跳过的 binlog event 数量。

#### cost of binlog event transform

query: `histogram_quantile(0.90, sum(rate(dm_syncer_binlog_transform_cost_bucket{task="$task", instance="$instance"}[1m])) by (le))`

说明：syncer 解析并且转换 binlog 成 SQLs 的耗时，单位：秒。

#### total sqls jobs

query: `rate(dm_syncer_added_jobs_total{task="$task", instance="$instance"}[1m])`

说明：单位时间内新增的 job 数量。

#### finished sqls jobs

query: `rate(dm_syncer_finished_jobs_total{task="$task", instance="$instance"}[1m])`

说明：单位时间内完成的 job 数量。

#### execution latency

query: `histogram_quantile(0.90, sum(rate(dm_syncer_txn_duration_time_bucket{task="$task", instance="$instance"}[1m])) by (le))`

说明：syncer 执行 transaction 到下游的耗时，单位：秒。

 
