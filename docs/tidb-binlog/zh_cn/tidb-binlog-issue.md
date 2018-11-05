---
title: tidb-binlog 问题反馈及排查流程
category: tools
---

# 问题反馈及排查流程

## 检查 TiDB-Binlog 运行情况

在上报问题前，请根据 checklist 列出的事项对 binlog 的运行情况进行检查。

### kafka 版本
* TiDB 是否开启了 Binlog
    * 查看 TiDB 启动脚本及配置，或者直接查看 TiDB 的日志中记录的配置参数，开启 binlog 的命令行参数为 binlog-socket，在配置文件中为 [binlog] 中的 socket。
* 查看 Pump、Drainer 的监控
    * 是否有指标异常。
* 查看所有的 Pump、Drainer 状态是否正常
    * 使用 binlogctl 工具查看，需要检查的事项包括：
        * 状态是否符合预期，正常工作时 offline 为 false。
        * Pump 的 kafka offset 位置是否正常（大于0，且与业务的数据量相符）。
        * Pump 的 latest local file pos 纪录的文件是否被 gc 处理。
        * 是否有部署列表之外的 Pump／Drainer。
* 查看 Pump 和 Drainer 的日志
    * 查看 Pump 和 Drainer 日志有没有 error、warning。
* 检查同步的表是否有 primary key 或者 unique key。

### cluster 版本
* TiDB 是否开启了 Binlog
    * 查看 TiDB 启动脚本及配置，或者直接查看 TiDB 的日志中记录的配置参数，开启 binlog 的命令行参数为 enable-binlog，在配置文件中为 [binlog] 中的 enable。
* 查看 Pump、Drainer 的监控
    * 是否有指标异常，参考 [监控指标说明](./tidb-binlog-monitor.md)
* 查看所有的 Pump、Drainer 状态是否正常
    * 使用 binlogctl 工具查看，需要检查的事项包括：
        * 状态是否符合预期，状态的详细说明参考 [Pump／Drainer 状态](./tidb-binlog-cluster.md#pumpdrainer-状态)。
        * 是否有部署列表之外的 Pump／Drainer。
        * Pump 的 commit ts 是否正常（大于0，且 ts 符合预期）。
* 查看 Pump 和 Drainer 的日志
    * 查看 Pump 和 Drainer 日志有没有 error、warning。
* 检查同步的表是否有 primary key 或者 unique key。

## 收集信息

### 基本信息

基本信息是必须要提供的信息，包括：
#### 用户信息
用户的业务场景,视具体情况提供信息。
#### 版本号

binlog 总共包含三个版本，local、kafka、新版本，首先应该说明属于哪个版本（2.1 GA 版本后 binlog 会跟 TiDB 一起管理版本），然后提供 commit hash 信息。

### 其他信息
除了基本信息，还需要根据具体的用户反馈提供相应的信息。

#### case 1：checklist 中所有检查都正常，pump 也是正常的，但是 drainer 不同步数据
Drainer 开启 debug 日志，运行一段时间（10分钟以上）提供日志
获取 groutine 信息，使用命令 curl http://127.0.0.1:8250/debug/pprof/goroutine\?debug\=2 > debug.log｀，提供 debug.log

#### case 2:  pump 或者 drainer 有报错日志
提供报错日志，包括上下文日志
如果报错日志中包含 "sarama"、"kafka"，提供 pump 和 drainer 中 kafka 相关的配置，以及用户使用的 kafka 版本号

#### case 3: drainer 同步数据太慢
目前写入速度大约有多少
提供 drainer 监控截图，主要为 event 的监控图
提供 drainer 的配置文件
确定用户的业务场景，包括：
    * 检查同步的表是否有 primary key 或者 unique key。
    * TiDB 是否在 load 数据／批量插入数据。
    * 是否会修改主键，如果是的话，确定修改主键的行为是否频繁。
    * 业务每秒大约多少条写入
    * 服务器配置、部署相关信息，包括是否在一个机房，网络延迟情况，下游的服务器配置

#### case 4: 待补充

## 建立 Issue

### 问题描述示例模版：

用户场景：

用户测试新版本 binlog，使用 ansible 部署了一套测试集群，总共两个 pump，drainer 同步到 kafka 中，然后从 kafka 读出数据写入到 hive 中。

问题描述：

drainer 中出现报错：（error 日志片段或者截图）
2018/10/22 21:03:31 consumer.go:330: [sarama] kafka: error while consuming 6502327210476397260_10-30-1-10_8250/0: kafka: error
decoding packet: CRC didn't match expected 0x0 got 0xe38a6876

信息：

版本：新版本 binlog，githash：123456

用户配置：（drainer 的配置信息）

kafka 版本：2.0.0
日志：（粘贴相关的日志，或者把日志文件／截图加到附件中）

## 问题反馈及处理流程
* 按照上面提供的方式收集排查问题需要的信息。
* 建立 Issue。
* 排查问题可能还需要其他信息，在 Issue 中以评论的方式交流。
* 解决问题后，在 Issue 评论中总结问题的具体原因和解决方式。