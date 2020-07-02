#!/bin/sh

set -e

cd "$(dirname "$0")"

OUT_DIR=/tmp/tidb_tools_test/sync_diff_inspector

mkdir $OUT_DIR || true

echo "use importer to generate test data"
mysql -uroot -h 127.0.0.1 -P 4000 -e "create database if not exists diff_test"
# TODO: run `importer -t "create table diff_test.test(\`table\` int, b varchar(10), c float, d datetime, primary key(a));" -c 10 -n 10000 -P 4000 -h 127.0.0.1 -D diff_test -b 1000`
# will exit with parser error, need to fix it in importer later, just change column name by mysql client now
importer -t "create table diff_test.test(a int, b varchar(10), c float, d datetime, primary key(a));" -c 10 -n 10000 -P 4000 -h 127.0.0.1 -D diff_test -b 1000
mysql -uroot -h 127.0.0.1 -P 4000 -e "alter table diff_test.test change column a \`table\` int"

echo "dump data and then load to tidb"
mydumper --host 127.0.0.1 --port 4000 --user root --outputdir $OUT_DIR/dump_diff -B diff_test -T test
loader -h 127.0.0.1 -P 4001 -u root -d $OUT_DIR/dump_diff

echo "use sync_diff_inspector to compare data"
sync_diff_inspector --config=./config_base.toml > $OUT_DIR/diff.log
check_contains "check pass!!!" $OUT_DIR/diff.log

echo "analyze table, and will use tidb's statistical information to split chunks"
check_contains "will split chunk by random again" $OUT_DIR/diff.log
mysql -uroot -h 127.0.0.1 -P 4000 -e "analyze table diff_test.test"
sync_diff_inspector --config=./config_base.toml > $OUT_DIR/diff.log
check_contains "check pass!!!" $OUT_DIR/diff.log
check_not_contains "will split chunk by random again" $OUT_DIR/diff.log

echo "test 'exclude-tables' config"
mysql -uroot -h 127.0.0.1 -P 4000 -e "create table if not exists diff_test.should_not_compare (id int)"
sync_diff_inspector --config=./config_base.toml > $OUT_DIR/diff.log
# doesn't contain the table's result in check report
check_not_contains "[table=should_not_compare]" $OUT_DIR/diff.log

mysql -uroot -h 127.0.0.1 -P 4000 -e "select state from sync_diff_inspector.summary where \`schema\`='diff_test' and \`table\`='test'" | grep "success"
if [ $? == 1 ]; then
    echo "the state is not success in summary table"
    exit 1
fi

for script in ./*/run.sh; do
    test_name="$(basename "$(dirname "$script")")"
    echo "---------------------------------------"
    echo "Running test $script..."
    echo "---------------------------------------"
    cp config_base.toml $test_name/
    sh "$script"
done
