#!/bin/sh

set -e

cd "$(dirname "$0")"

OUT_DIR=/tmp/tidb_tools_test/sync_diff_inspector/output
rm -rf $OUT_DIR

echo "generate data to sharding tables"
mysql -uroot -h ${MYSQL_HOST} -P ${MYSQL_PORT} -e "create table diff_test.shard_test1(\`table\` int, b varchar(10), c float, d datetime, primary key(\`table\`));"
mysql -uroot -h ${MYSQL_HOST} -P ${MYSQL_PORT} -e "create table diff_test.shard_test2(\`table\` int, b varchar(10), c float, d datetime, primary key(\`table\`));"

# each table only have part of data
mysql -uroot -h ${MYSQL_HOST} -P ${MYSQL_PORT} -e "insert into diff_test.shard_test1 (\`table\`, b, c, d) SELECT \`table\`, b, c, d FROM diff_test.test WHERE \`table\`%2=0"
mysql -uroot -h ${MYSQL_HOST} -P ${MYSQL_PORT} -e "insert into diff_test.shard_test2 (\`table\`, b, c, d) SELECT \`table\`, b, c, d FROM diff_test.test WHERE \`table\`%2=1"

sed "s/\"127.0.0.1\"#MYSQL_HOST/\"${MYSQL_HOST}\"/g" ./config_base.toml | sed "s/3306#MYSQL_PORT/${MYSQL_PORT}/g" > ./config.toml

echo "compare sharding tables with one table in downstream, check result should be pass"
sync_diff_inspector --config=./config.toml > $OUT_DIR/shard_diff.output
check_contains "check pass!!!" $OUT_DIR/sync_diff.log
rm -f $OUT_DIR/sync_diff.log

echo "update data in one shard table, and data should not be equal"
mysql -uroot -h ${MYSQL_HOST} -P ${MYSQL_PORT} -e "update diff_test.shard_test1 set b = 'abc' limit 1"
sync_diff_inspector --config=./config.toml > $OUT_DIR/shard_diff.output || true
check_contains "check failed" $OUT_DIR/sync_diff.log
rm -f $OUT_DIR/sync_diff.log

echo "shard test passed"