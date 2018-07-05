## binlogctl

binlogctl is a tool for perform some tidb-binlog related operations, like query status pump/drainer and unregister someone pump/drainer.

## How to use

```
Usage of binlogctl:
	-V	prints version and exit
	-cmd string
		operator: "generate_meta", "pumps", "drainers", "delete-pump", "delete-drainer" (default "pumps")
	-data-dir string
		meta directory path (default "binlog_position")
	-node-id string
		id of node, use to delete some node with operation delete-pump and delete-drainer
	-pd-urls string
		a comma separated list of PD endpoints (default "http://127.0.0.1:2379")
	-ssl-ca string
		Path of file that contains list of trusted SSL CAs for connection with cluster components.
	-ssl-cert string
		Path of file that contains X509 certificate in PEM format for connection with cluster components.
	-ssl-key string
		Path of file that contains X509 key in PEM format for connection with cluster components.
	-time-zone Asia/Shanghai
		set time zone if you want save time info in savepoint file, for example Asia/Shanghai for CST time, `Local` for local time
```
## Download Binary ( CentOS 7+ platform )

```bash
# Download the tool package.
wget http://download.pingcap.org/tidb-tools-latest-linux-amd64.tar.gz
wget http://download.pingcap.org/tidb-tools-latest-linux-amd64.sha256

# Check the file integrity. If the result is OK, the file is correct.
sha256sum -c tidb-tools-latest-linux-amd64.sha256

# Extract the package.
tar -xzf tidb-tools-latest-linux-amd64.tar.gz
cd tidb-tools-latest-linux-amd64
```

## Example



### query pumps/drainers
```
bin/binlogctl -pd-urls=http://127.0.0.1:2379 -cmd pumps/drainers
```
Then the result will be like this:
```
2018/06/21 11:24:10 nodes.go:53: [info] pump: &{NodeID:ip-192-168-199-118:8250 Host:127.0.0.1:8250 IsAlive:true IsOffline:false LatestFilePos:{Suffix:0 Offset:15320} LatestKafkaPos:{Suffix:0 Offset:382} OfflineTS:0}
```
we would format output later :)


### unregister pump/drainer
```
bin/binlogctl -pd-urls=http://127.0.0.1:2379 -cmd delete-pump/delete-drainer -node-id ip-127-0-0-1:8250/{nodeID}
```

### generate meta
meta contains commit TS that can be used to specifies the location of the synchronized data
TODO: improve meta later, like adding offset of kafka topic that corresponds to each pump node

```
bin/binlogctl -pd-urls=http://127.0.0.1:2379 -cmd generate_meta
```
Then the result will be like this:
```
INFO[0000] [pd] create pd client with endpoints [http://192.168.199.118:32379]
INFO[0000] [pd] leader switches to: http://192.168.199.118:32379, previous:
INFO[0000] [pd] init cluster id 6569368151110378289
2018/06/21 11:24:47 meta.go:117: [info] meta: &{CommitTS:400962745252184065}
```
It would also generate a `{data-dir}/savepoint` meta file

## License
Apache 2.0 license. See the [LICENSE](../LICENSE) file for details.
