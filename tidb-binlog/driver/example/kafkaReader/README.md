### consume and parse kafka binlog message for java demo

#### Env：
tidb: v3.0.0 <br/>
drainer: v3.0.0 <br/>
kafka: kafka_2.12 1.0.0 <br/>
local windows environment protobuf version：protoc-3.9.1-win64 <br/>
[binlog.proto](https://github.com/pingcap/tidb-tools/blob/master/tidb-binlog/slave_binlog_proto/proto/binlog.proto) use official point file。

#### Execute protoc command to  generate java file：
protoc  --java_out=src/main/java src/main/resources/proto/descriptor.proto    --proto_path=src/main/resources/proto/  <br/>
protoc  --java_out=src/main/java src/main/resources/proto/gogo.proto    --proto_path=src/main/resources/proto/  <br/>
protoc  --java_out=src/main/java src/main/resources/proto/binlog.proto  --proto_path=src/main/resources/proto/  <br/>

#### How to run:
in intel idea ide， run Booter.java main method。<br/>
point Booter.topic、Booter.serever、Booter.offset and run it。

 #### Verify the correctness of the binlog in kafka using kafka_reader
 The kafka_reader file is compiled from the go language and is used to parse the binlog log in kafka in the form of a command to aid validation。  <br/>

 how to run：<br/>
 ./kafka_reader -offset=-1 -topic=6717826900501472462_obinlog  -kafkaAddr=192.168.138.22:9092 -commitTS=-1  -clusterID=6717826900501472462  <br/>

offset：kafka consume offset <br/>
topic：topic name  <br/>
kafkaAddr：kafka's broker address, specify one  <br/>
commitTS： binlog‘s commitTS, generally -1  <br/>
clusterID：tidb cluster id  <br/>

The above parameters are required。
