数据流过程
===

### SQL / binlog 数据流过程

#### 全量数据

1. dump unit（mydumper） 从上游 MySQL / MariaDB 导出数据以 SQL 文件形式存储在指定目录
2. load unit（loader） 将导出的 SQL 文件数据导入到下游 TiDB

#### 增量数据

1. relay log 处理单元从上游 MySQL / MariaDB 获取 binlog 并以相同的格式存储在指定目录
2. binlog replication/syncer 处理单元读取 relay binlog 并转换成 SQL 导入到下游 TiDB
- 此过程与 MySQL slave relay log 的读写及执行类似，参见 <https://dev.mysql.com/doc/refman/5.7/en/slave-logs-relaylog.html>。

### 控制信息数据流过程

#### 控制命令

1. 用户通过 dmctl 执行 `start-task`, `query-status` 等命令
2. dmctl 根据命令及参数构造控制信息发送给 dm-master
3. dm-master （解析或重新构造部分控制信息后）将控制信息分发给指定的一个或多个 dm-worker
4. dm-worker 响应控制信息请求，并构造响应信息发回给 dm-master
5. dm-master 收集各 dm-worker 的响应信息并进行组合
6. dm-master 将组合后的响应信息发回给 dmctl

#### sharding DDL 信息

1. dm-worker 将 sharding DDL 相关信息（DDL, schema, table 等）发送给 dm-master
2. dm-master 根据 DDL 信息构造对应的 DDL lock，并将 lock 信息发回给 dm-worker
3. dm-worker 保存 dm-master 发回的 lock 信息
4. dm-master 收集各 dm-worker 发来的 DDL 信息并判断 DDL lock 同步成功
5. dm-master 构造执行 DDL 请求的信息发送给 lock 的 owner（第一个发送 DDL 信息的 dm-worker）
6. owner 将 DDL 执行结果发送给 dm-master
7. dm-master 根据 owner 发回的 DDL 执行结果进行判断
   1. 如果执行失败，结束本次同步处理，等待定时重试或用户手动处理
   2. 如果执行成功，继续后续流程
8. dm-master 构造跳过 DDL 请求的信息发送给其他 dm-worker（非 owner）
9. 各 dm-worker 尝试跳过 DDL 并将结果发送给 dm-master
10. dm-master 根据各 dm-worker 发回的结果组合成 DDL 同步结果记录在 log 中
