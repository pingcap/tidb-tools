#!/bin/sh

set -e

cd "$(dirname "$0")"

OUT_DIR=/tmp/tidb_tools_test/sync_diff_inspector

echo "generate data to sharding tables"
mysql -uroot -h 127.0.0.1 -P 4001 -e "create table diff_test.shard_test1(\`table\` int, b varchar(10), c float, d datetime, primary key(\`table\`));"
mysql -uroot -h 127.0.0.1 -P 4001 -e "create table diff_test.shard_test2(\`table\` int, b varchar(10), c float, d datetime, primary key(\`table\`));"

# each table only have part of data
mysql -uroot -h 127.0.0.1 -P 4001 -e "insert into diff_test.shard_test1 (\`table\`, b, c, d) SELECT \`table\`, b, c, d FROM diff_test.test WHERE \`table\`%2=0"
mysql -uroot -h 127.0.0.1 -P 4001 -e "insert into diff_test.shard_test2 (\`table\`, b, c, d) SELECT \`table\`, b, c, d FROM diff_test.test WHERE \`table\`%2=1"


echo "compare sharding tables with one table in downstream, check result should be pass"
sync_diff_inspector --config=./config.toml > $OUT_DIR/shard_diff.log
check_contains "check pass!!!" $OUT_DIR/shard_diff.log

echo "update data in one shard table, and data should not be equal"
mysql -uroot -h 127.0.0.1 -P 4001 -e "update diff_test.shard_test1 set b = 'abc' limit 1"
sync_diff_inspector --config=./config.toml > $OUT_DIR/shard_diff.log || true
check_contains "check failed" $OUT_DIR/shard_diff.log

echo "shard test passed"