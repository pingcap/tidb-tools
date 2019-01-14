迁移 DM-worker
===

当 DM-worker 不可用时，需要将在不可用的 DM-worker 上运行的任务迁移到另一台 DM-worker上

### 迁移流程
1. 部署新的 DM-worker
2. 确定不可用的 DM-worker 同步点
2. 对新部署的 DM-worker 使用 `migrate-relay` 命令，接上一个 DM-worker 的同步点

### 如何确定不可用的 DM-worker 同步点
通常会保存在下游数据库里，task配置里的`meta-schema`字段指名了保存在下游数据库的位置。

这里假设`meta_schema`的值是`dm_meta`， 任务名是`test`就到下游数据库中找到`dm_meta`这个库中，
找到`test_syncer_checkpoint`这个表, 用SQL查询出所有的同步点
`select binlog_name, binlog_pos from dm_meta.test_syncer_checkpoint where id = 要迁移的 DM-worker 的id and is_global = 1;` 
选取最近确认的同步点， 如果 DM-worker 上运行着多个任务, 需要找到所有任务的同步点，取其中时间最早的同步点。

### 同步点包括哪些信息
需要 binlog-name 和 binlog-pos
参考[relay-log](../features/relay-log.md)

### 命令参数解释
`migrate-relay <worker> <binlogName> <binlogPos>`
- `worker`: 新部署的 DM-worker 的全局ID
- `binlogName`: 对应同步点的`binlog-name`
- `binlogPos`: 对应同步点的`binlog-pos`