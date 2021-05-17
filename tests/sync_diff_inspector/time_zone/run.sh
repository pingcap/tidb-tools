#!/bin/sh

set -e

cd "$(dirname "$0")"
OUT_DIR=/tmp/tidb_tools_test/sync_diff_inspector

mysql -uroot -h 127.0.0.1 -P 4000 -e "SET @@GLOBAL.SQL_MODE='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';"

mysql -uroot -h 127.0.0.1 -P 4000 -e "create database if not exists tz_test"
mysql -uroot -h 127.0.0.1 -P 4000 -e "create table tz_test.diff(id int, dt datetime, ts timestamp);"
mysql -uroot -h 127.0.0.1 -P 4000 -e "insert into tz_test.diff values (1, now(), now());"

echo "dump data and then load to tidb"
rm -rf $OUT_DIR/dump_tz_diff
mydumper --host 127.0.0.1 --port 4000 --user root --outputdir $OUT_DIR/dump_tz_diff -B tz_test -T diff
loader -h 127.0.0.1 -P 4001 -u root -d $OUT_DIR/dump_tz_diff

echo "check with the same time_zone, check result should be pass"
sync_diff_inspector --config=./config.toml > $OUT_DIR/time_zone_diff.log
check_contains "check pass!!!" $OUT_DIR/time_zone_diff.log

# check upstream and downstream time_zone
mysql -uroot -h 127.0.0.1 -P 4000 -e "SET @@global.time_zone = '+08:00'";
mysql -uroot -h 127.0.0.1 -P 4001 -e "SET @@global.time_zone = '+00:00'";
echo "check with different time_zone, check result should be pass again"
sync_diff_inspector --config=./config.toml > $OUT_DIR/time_zone_diff.log
check_contains "check pass!!!" $OUT_DIR/time_zone_diff.log

# reset time_zone
mysql -uroot -h 127.0.0.1 -P 4000 -e "SET @@global.time_zone = 'SYSTEM'";
mysql -uroot -h 127.0.0.1 -P 4001 -e "SET @@global.time_zone = 'SYSTEM'";
