#!/bin/sh

set -e

cd "$(dirname "$0")"

OUT_DIR=/tmp/tidb_tools_test/sync_diff_inspector/output
rm -rf $OUT_DIR
mkdir -p $OUT_DIR

mysql -uroot -h ${MYSQL_HOST} -P ${MYSQL_PORT} < ./data.sql

# tidb
mysql -uroot -h 127.0.0.1 -P 4000 < ./data.sql

sed "s/\"127.0.0.1\"#MYSQL_HOST/\"${MYSQL_HOST}\"/g" ./config_base.toml | sed "s/3306#MYSQL_PORT/${MYSQL_PORT}/g" > ./config.toml

echo "compare json tables, check result should be pass"
sync_diff_inspector --config=./config.toml > $OUT_DIR/json_diff.output
check_contains "check pass!!!" $OUT_DIR/sync_diff.log
rm -rf $OUT_DIR/*

echo "update data to make it different, and data should not be equal"
mysql -uroot -h ${MYSQL_HOST} -P ${MYSQL_PORT} -e "insert into json_test.test values (5, '{\"id\": 5, \"bool\": true, \"name\":\"aaa\"}');"
mysql -uroot -h 127.0.0.1 -P 4000 -e "insert into json_test.test values (5, '{\"id\": 5, \"bool\": false, \"name\":\"aaa\"}');"
sync_diff_inspector --config=./config.toml > $OUT_DIR/json_diff.output || true
check_contains "check failed" $OUT_DIR/sync_diff.log
rm -rf $OUT_DIR/*

echo "update data to make it different, and downstream json data is NULL"
mysql -uroot -h 127.0.0.1 -P 4000 -e "replace into json_test.test values (5, NULL);"
sync_diff_inspector --config=./config.toml > $OUT_DIR/json_diff.output || true
check_contains "check failed" $OUT_DIR/sync_diff.log
rm -rf $OUT_DIR/*
