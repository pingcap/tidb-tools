#!/bin/sh

set -e

cd "$(dirname "$0")"
OUT_DIR=/tmp/tidb_tools_test/sync_diff_inspector

mysql -uroot -h 127.0.0.1 -P 4000 -e "SET @@GLOBAL.SQL_MODE='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';"
sleep 3

for port in 4000 4001; do
  mysql -uroot -h 127.0.0.1 -P $port -e "create database if not exists tz_test"
  mysql -uroot -h 127.0.0.1 -P $port -e "create table tz_test.diff(id int, dt datetime, ts timestamp);"
  mysql -uroot -h 127.0.0.1 -P $port -e "insert into tz_test.diff values (1, '2020-05-17 09:12:13', '2020-05-17 09:12:13');"
done

echo "check with the same time_zone, check result should be pass"
sync_diff_inspector --config=./config.toml > $OUT_DIR/time_zone_diff.log
check_contains "check pass!!!" $OUT_DIR/time_zone_diff.log

# check upstream and downstream time_zone
mysql -uroot -h 127.0.0.1 -P 4000 -e "SET @@global.time_zone = '+08:00'";
mysql -uroot -h 127.0.0.1 -P 4001 -e "SET @@global.time_zone = '+00:00'";
sleep 5

echo "check with different time_zone, check result should be pass again"
sync_diff_inspector --config=./config.toml > $OUT_DIR/time_zone_diff.log
check_contains "check pass!!!" $OUT_DIR/time_zone_diff.log

# reset time_zone
mysql -uroot -h 127.0.0.1 -P 4000 -e "SET @@global.time_zone = 'SYSTEM'";
mysql -uroot -h 127.0.0.1 -P 4001 -e "SET @@global.time_zone = 'SYSTEM'";
