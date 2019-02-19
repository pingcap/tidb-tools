Change Log
===

### 2018-11-06

#### bug 修复

- 修复下游 PD，TiKV 等 timeout 时 binlog replication 处理单元未 retry 的问题
- 修复 loader 导入大量 SQL 文件遇到错误后未能正确 pause 的问题
- 修复部分 DDL statement 内包含注释造成 route rename 出错的问题

### 2018-11-05

### bug 修复

- 修复无法跳过与内置 SQL 模式（如 `FLUSH`）匹配的 DDL 的问题

### 2018-11-04

#### 功能改进

- binlog replication 处理单元在没有数据同步时定时 flush checkpoint
- 在 binlog replication 处理单元的 status 中增加输出上下游是否已经同步的标识 `synced`

### 2018-11-02

#### 功能改进

- 完善了 binlog replication 处理单元读取 relay log 的逻辑

#### bug 修复

- 修复 SQL 执行出错时更新 checkpoint 错误的问题
- 修复 relay log 在 checksum 开启时对于 fill gap 的 events 未重新计算 checksum 的问题

### 2018-10-31

#### bug 修复

- 修复 skip DML 时断点可能更新错误的问题

### 2018-10-30

#### 功能改进

- 增加 paused 状态、binlog file gap 等 metrics 监控与告警

### 2018-10-29

#### 功能新增

- pre-check 增加检查上游数据库是否存在大小写同名的表
- 支持使用 dmctl 热更新 DM-master 配置

#### 功能改进

- column mapping 支持仅作用于 instance / schema

#### bug 修复

- 修复 metrics 监控与告警的一些表达式问题

### 2018-10-26

#### 功能改进

- 支持 pt online DDL 并发操作

#### bug 修复

- 写本地 relay log 时跳过过时的 events；当缺失 events 时报错

### 2018-10-25

#### 功能改进

- 完善 dmctl 的运行模式，拆分命令到不同的运行模式中

### 2018-10-24

#### 功能新增

- relay 支持通过 dmctl 暂停 / 恢复运行
- 开始/恢复 relay 拉取流程时，如果连接到新 master server，则自动进行 relay 的主-从切换

#### bug 修复

- relay 在主-从切换后，为新 relay log file 填充同步起始点前的 events

### 2018-10-23

#### 功能改进
- relay 连上游读 binlog 时增加超时、心跳设置

#### bug 修复

- 写本地 relay log 时忽略一些 dummy events

### 2018-10-20

#### 功能改进

- task 与 DM-worker 重合的配置信息完善

### 2018-10-19

#### 功能新增

- relay 主-从切换 支持

### 2018-10-18

#### 功能新增

- pt online DDL 支持

#### 功能改进

- 用户可配置是否启用 `ansi-quotes` `sql_mode`

#### bug 修复

- 修复在 skip query 时断点信息更新错误的问题

### 2018-10-17

#### bug 修复

- 修复 binlog replication 处理单元在非 sharding DDL 模式时 query status 会导致 panic 的问题

### 2018-10-15

#### bug 修复

- 修复 loader unit 在暂停任务、停止任务时并发控制错误导致 panic 的问题
