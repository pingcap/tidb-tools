#!/bin/sh

set -e

cd "$(dirname "$0")"

OUT_DIR=/tmp/tidb_tools_test/sync_diff_inspector

mkdir $OUT_DIR || true

echo "use importer to generate test data"
mysql -uroot -h 127.0.0.1 -P 4000 -e "create database if not exists diff_test"
importer -t "create table diff_test.test(a int, b varchar(10), c float, d datetime, primary key(a));" -c 10 -n 10000 -P 4000 -h 127.0.0.1 -D diff_test -b 1000

echo "dump data and then load to tidb"
mydumper --host 127.0.0.1 --port 4000 --user root --outputdir $OUT_DIR/dump_diff -B diff_test -T test
loader -h 127.0.0.1 -P 4001 -u root -d $OUT_DIR/dump_diff

echo "use sync_diff_inspector to compare data"
sync_diff_inspector --config=./config_base.toml > $OUT_DIR/diff.log
check_contains "test pass!!!" $OUT_DIR/diff.log

echo "analyze table, and will use tidb's statistical information to split chunks"
check_contains "will split chunk by random again" $OUT_DIR/diff.log
mysql -uroot -h 127.0.0.1 -P 4000 -e "analyze table diff_test.test"
sync_diff_inspector --config=./config_base.toml > $OUT_DIR/diff.log
check_contains "test pass!!!" $OUT_DIR/diff.log
check_not_contains "will split chunk by random again" $OUT_DIR/diff.log

for script in ./*/run.sh; do
    test_name="$(basename "$(dirname "$script")")"
    echo "---------------------------------------"
    echo "Running test $script..."
    echo "---------------------------------------"
    cp config_base.toml $test_name/
    sh "$script"
done