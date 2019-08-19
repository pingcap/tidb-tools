
#!/bin/sh

set -e

cd "$(dirname "$0")"

OUT_DIR=/tmp/tidb_tools_test/sync_diff_inspector

echo "update data in column b, data should not be equal"
mysql -uroot -h 127.0.0.1 -P 4000 -e "update diff_test.test set b = 'abc' limit 1"

rm $OUT_DIR/fix.sql || true
sync_diff_inspector --config=./config_base.toml > $OUT_DIR/ignore_column_diff.log || true
check_contains "sourceDB don't equal targetDB" $OUT_DIR/ignore_column_diff.log
mv $OUT_DIR/fix.sql $OUT_DIR/fix.sql.bak

echo "ignore check column b, check result should be pass"
sync_diff_inspector --config=./config.toml > $OUT_DIR/ignore_column_diff.log || true
check_contains "test pass!!!" $OUT_DIR/ignore_column_diff.log

echo "execute fix.sql and use base config, and then compare data, data should be equal"
cat $OUT_DIR/fix.sql.bak | mysql -uroot -h127.0.0.1 -P 4000
sync_diff_inspector --config=./config_base.toml > $OUT_DIR/ignore_column_diff.log
check_contains "test pass!!!" $OUT_DIR/ignore_column_diff.log

echo "ignore_column test passed"
