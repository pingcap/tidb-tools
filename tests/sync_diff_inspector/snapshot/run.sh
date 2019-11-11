
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

echo "use snapshot compare data, test sql mode by the way, will return error, diff should not passed"
mysql -uroot -h 127.0.0.1 -P 4000 -e "SET GLOBAL sql_mode = 'ANSI_QUOTES';"
sleep 10
mysql -uroot -h 127.0.0.1 -P 4000 -e "show variables like '%sql_mode%'"
mysql -uroot -h 127.0.0.1 -P 4000 -e "show create table diff_test.test"
cp config_base.toml config.toml
echo "snapshot = \"$ts\"" >> config.toml
sync_diff_inspector --config=./config.toml > $OUT_DIR/snapshot_diff.log || true
check_contains "get table diff_test.test's inforamtion error" $OUT_DIR/snapshot_diff.log

echo "use snapshot compare data, data should be equal"
echo "sql-mode = 'ANSI_QUOTES'" >> config.toml
sync_diff_inspector --config=./config.toml > $OUT_DIR/snapshot_diff.log
check_contains "test pass!!!" $OUT_DIR/snapshot_diff.log

echo "execute fix.sql and use base config, and then compare data, data should be equal"
cat $OUT_DIR/fix.sql.bak | mysql -uroot -h127.0.0.1 -P 4000
echo "sql-mode = 'ANSI_QUOTES'" >> config_base.toml
sync_diff_inspector --config=./config_base.toml > $OUT_DIR/snapshot_diff.log
check_contains "test pass!!!" $OUT_DIR/snapshot_diff.log

# reset sql mode
mysql -uroot -h 127.0.0.1 -P 4000 -e "SET GLOBAL sql_mode = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION';"

echo "snapshot test passed"