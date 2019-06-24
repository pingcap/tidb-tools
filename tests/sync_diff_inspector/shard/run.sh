#!/bin/sh

set -e

cd "$(dirname "$0")"

OUT_DIR=/tmp/tidb_tools_test/sync_diff_inspector

echo "generate data to sharding tables"
mysql -uroot -h 127.0.0.1 -P 4001 -e "create table diff_test.shard_test1(a int, b varchar(10), c float, d datetime, primary key(a));"
mysql -uroot -h 127.0.0.1 -P 4001 -e "create table diff_test.shard_test2(a int, b varchar(10), c float, d datetime, primary key(a));"

# each table only have part of data
mysql -uroot -h 127.0.0.1 -P 4001 -e "insert into diff_test.shard_test1 (a, b, c, d) SELECT a, b, c, d FROM diff_test.test WHERE a%2=0"
mysql -uroot -h 127.0.0.1 -P 4001 -e "insert into diff_test.shard_test2 (a, b, c, d) SELECT a, b, c, d FROM diff_test.test WHERE a%2=1"


echo "compare sharding tables with one table in downstream, check result should be pass"
sync_diff_inspector --config=./config.toml > $OUT_DIR/shard_diff.log
check_contains "test pass!!!" $OUT_DIR/shard_diff.log

echo "update data in one shard table, and data should not be equal"
mysql -uroot -h 127.0.0.1 -P 4001 -e "update diff_test.shard_test1 set b = 'abc' limit 1"
sync_diff_inspector --config=./config.toml > $OUT_DIR/shard_diff.log || true
check_contains "sourceDB don't equal targetDB" $OUT_DIR/shard_diff.log

echo "shard test passed"