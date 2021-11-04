#!/bin/sh

set -e

cd "$(dirname "$0")"
OUT_DIR=/tmp/tidb_tools_test/sync_diff_inspector/output
rm -rf $OUT_DIR
mkdir -p $OUT_DIR

mysql -uroot -h 127.0.0.1 -P 4000 -e "SET @@GLOBAL.SQL_MODE='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';"
sleep 3

for port in 4000 4001; do
  mysql -uroot -h 127.0.0.1 -P $port -e "create database if not exists tz_test"
  mysql -uroot -h 127.0.0.1 -P $port -e "create table tz_test.diff(id int, dt datetime, ts timestamp);"
  mysql -uroot -h 127.0.0.1 -P $port -e "insert into tz_test.diff values (1, '2020-05-17 09:12:13', '2020-05-17 09:12:13');"
  mysql -uroot -h 127.0.0.1 -P $port -e "set @@session.time_zone = \"-07:00\"; insert into tz_test.diff values (2, '2020-05-17 09:12:13', '2020-05-17 09:12:13');"
done

echo "check with the same time_zone, check result should be pass"
sync_diff_inspector --config=./config.toml > $OUT_DIR/time_zone_diff.output
check_contains "check pass!!!" $OUT_DIR/sync_diff.log
rm -rf $OUT_DIR/*

# check upstream and downstream time_zone
mysql -uroot -h 127.0.0.1 -P 4000 -e "SET @@global.time_zone = '+08:00'";
mysql -uroot -h 127.0.0.1 -P 4001 -e "SET @@global.time_zone = '+00:00'";
sleep 5

echo "check with different time_zone, check result should be pass again"
sync_diff_inspector --config=./config.toml > $OUT_DIR/time_zone_diff.output
check_contains "check pass!!!" $OUT_DIR/sync_diff.log
rm -rf $OUT_DIR/*

echo "set different rows"
mysql -uroot -h 127.0.0.1 -P 4001 -e "SET @@session.time_zone = '-06:00'; insert into tz_test.diff values (4, '2020-05-17 09:12:13', '2020-05-17 09:12:13');"
sync_diff_inspector --config=./config.toml > $OUT_DIR/time_zone_diff.output
check_contains "check failed" $OUT_DIR/sync_diff.log
rm -rf $OUT_DIR/*

echo "fix the rows"
cat $FIX_DIR/fix-on-tidb/*.sql | mysql -uroot -h127.0.0.1 -P 4000
sync_diff_inspector --config=./config.toml > $OUT_DIR/time_zone_diff.output
check_contains "check pass!!!" $OUT_DIR/sync_diff.log
rm -rf $OUT_DIR/*
mysql -uroot -h 127.0.0.1 -P 4000 -e "SET @@session.time_zone = '-06:00'; select ts from tz_test.diff where id = 4;" > $OUT_DIR/tmp_sql_timezone
check_contains "2020-05-17 09:12:13" $OUT_DIR/tmp_sql_timezone

# reset time_zone
mysql -uroot -h 127.0.0.1 -P 4000 -e "SET @@global.time_zone = 'SYSTEM'";
mysql -uroot -h 127.0.0.1 -P 4001 -e "SET @@global.time_zone = 'SYSTEM'";
