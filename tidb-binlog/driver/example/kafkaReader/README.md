### java解析Drainer推送到kafka里边的binlog日志

#### 环境：
tidb: v3.0.0 <br/>
drainer: v3.0.0 <br/>
kafka: kafka_2.12 1.0.0 <br/>
本地windows环境安装的protobuf版本：protoc-3.9.1-win64 <br/>
[binlog.proto](https://github.com/pingcap/tidb-tools/blob/master/tidb-binlog/slave_binlog_proto/proto/binlog.proto)是使用的官方指定的文件。

#### 使用protoc生成java文件：
protoc  --java_out=src/main/java src/main/resources/proto/descriptor.proto    --proto_path=src/main/resources/proto/  <br/>
protoc  --java_out=src/main/java src/main/resources/proto/gogo.proto    --proto_path=src/main/resources/proto/  <br/>
protoc  --java_out=src/main/java src/main/resources/proto/binlog.proto  --proto_path=src/main/resources/proto/  <br/>

#### 运行:
运行Booter启动。<br/>
指定Booter.topic、Booter.serever、Booter.offset启动即可。
 
 #### 使用kafka_reader验证kafka中的binlog的正确性
 kafka_reader文件是由go语言编译而成，用于用命令的形式解析kafka中的binlog日志，用来辅助验证的。  <br/>
 
 使用方式：<br/> 
 ./kafka_reader -offset=-1 -topic=6717826900501472462_obinlog  -kafkaAddr=192.168.138.22:9092 -commitTS=-1  -clusterID=6717826900501472462  <br/>

offset：kafka消费游标 <br/>
topic：主题名称  <br/>
kafkaAddr：kafka的broker，指定一个即可  <br/>
commitTS： binlog的commitTS，一般指定为-1  <br/>
clusterID：tidb集群id  <br/>

以上参数都是必填的。


