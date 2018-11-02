Change Log
===

### 2018-11-02

#### 功能改进

- 完善了 syncer 读取 relay log 的逻辑，[PR-401]

#### bug 修复

- 修复 SQL 执行出错时更新 checkpoint 错误的问题
- 修复 relay log 在 checksum 开启时对于 fill gap 的 events 未重新计算 checksum 的问题

### 2018-10-31

#### bug 修复

- 修复 skip DML 时断点可能更新错误的问题，[PR-400]

### 2018-10-30

#### 功能改进

- 增加 paused 状态、binlog file gap 等 metrics 监控与告警，[PR-399]

### 2018-10-29

#### 功能新增

- pre-check 增加检查上游数据库是否存在大小写同名的表，[PR-379]
- 支持使用 dmctl 热更新 dm-master 配置，[PR-377]

#### 功能改进

- column mapping 支持仅作用于 instance / schema，[PR-398]

#### bug 修复

- 修复 metrics 监控与报警的一些表达式问题，[PR-395]

### 2018-10-26

#### 功能改进

- 支持 pt online DDL 并发操作，[PR-392]

#### bug 修复

- relay 写本地 log 时检查 event 是否陈旧、是否缺失，[PR-393]

### 2018-10-25

#### 功能改进

- 完善 dmctl 的运行模式，拆分不同命令，[PR-390]

### 2018-10-24

#### 功能新增

- relay 支持通过 dmctl 暂停 / 恢复运行，[PR-382]
- 开始/恢复 relay 拉取流程时，如果连接到了新 master server，自动进行 relay 的主-从切换，[PR-389]

#### bug 修复

- relay 在主-从切换后，为新 relay log file 填充同步起始点前的 events，[PR-385]

### 2018-10-23

#### bug 修复

- relay 写本地 log 时忽略一些 dummy event，[PR-384]
- relay 连上游读 binlog 时增加超时、心跳设置，[PR-386]

### 2018-10-20

#### 功能改进

- task 与 dm-worker 重合配置信息完善，[PR-381]

### 2018-10-19

#### 功能新增

- relay 主-从切换 支持，[PR-330]

### 2018-10-18

#### 功能新增

- pt online DDL 支持，[PR-359]

#### 功能改进

- 是否启用 ansi-quotes sql_mode 用户可配置，[PR-378]

#### bug 修复

- 修复在 skip query 时断点信息更新错误的问题，[PR-380]

### 2018-10-17

#### bug 修复

- syncer unit 在非 sharding DDL 模式时 query status 会导致 panic，[PR-376]

### 2018-10-15

#### bug 修复

- loader unit 在暂停任务、停止任务时并发控制错误导致的 panic，[PR-374]
