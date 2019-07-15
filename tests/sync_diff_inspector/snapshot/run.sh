
#!/bin/sh

set -e

cd "$(dirname "$0")"

OUT_DIR=/tmp/tidb_tools_test/sync_diff_inspector

mysql -uroot -h 127.0.0.1 -P 4000 -e "show master status" > $OUT_DIR/ts.log
cat $OUT_DIR/ts.log
ts=`grep -oE "[0-9]+" $OUT_DIR/ts.log`
echo "get ts $ts"

echo "delete one data, diff should not passed"
mysql -uroot -h 127.0.0.1 -P 4000 -e "delete from diff_test.test limit 1"

sync_diff_inspector --config=./config_base.toml > $OUT_DIR/snapshot_diff.log || true
check_contains "sourceDB don't equal targetDB" $OUT_DIR/snapshot_diff.log
# fix.sql will be empty after check below, so backup it
cp $OUT_DIR/fix.sql $OUT_DIR/fix.sql.bak

echo "use snapshot compare data, data should be equal"
cp config_base.toml config.toml
echo "snapshot = \"$ts\"" >> config.toml
sync_diff_inspector --config=./config.toml > $OUT_DIR/snapshot_diff.log
check_contains "test pass!!!" $OUT_DIR/snapshot_diff.log

echo "execute fix.sql and use base config, and then compare data, data should be equal"
cat $OUT_DIR/fix.sql.bak | mysql -uroot -h127.0.0.1 -P 4000
sync_diff_inspector --config=./config_base.toml > $OUT_DIR/snapshot_diff.log
check_contains "test pass!!!" $OUT_DIR/snapshot_diff.log

echo "snapshot test passed"