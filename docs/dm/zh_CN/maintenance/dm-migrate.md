迁移dm-worker
===

当dm-worker不可用时，需要将在不可用的dm-worker上运行的任务迁移到另一台dm-worker上

### 迁移流程
1. 部署新的dm-worker
2. 确定不可用的dm-worker同步点
2. 对新部署的dm-worker使用`migrate-relay`命令，接上一个dm-worker的同步点

### 如何确定不可用的dm-worker同步点
通常会保存在下游数据库里，task配置里的`meta-schema`字段指名了保存在下游数据库的位置。

### 同步点包括哪些信息
需要binlog-name和binlog-pos
参考[relay-log](../features/relay-log.md)

### 命令参数解释
`migrate-relay <worker> <binlogName> <binlogPos>`
- `worker`: 新部署的dm-worker的全局ID
- `binlogName`: 对应同步点的`binlog-name`
- `binlogPos`: 对应同步点的`binlog-pos`