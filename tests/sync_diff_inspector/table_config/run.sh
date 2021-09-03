
#!/bin/sh

set -e

cd "$(dirname "$0")"

OUT_DIR=/tmp/tidb_tools_test/sync_diff_inspector/output
rm -rf $OUT_DIR
mkdir -p $OUT_DIR

echo "update data in column b (WHERE \`table\` >= 10 AND \`table\` <= 200), data should not be equal"
mysql -uroot -h 127.0.0.1 -P 4000 -e "update diff_test.test set b = 'abc' where \`table\` >= 10 AND \`table\` <= 200"

sync_diff_inspector --config=./config.toml > $OUT_DIR/ignore_column_diff.output || true
check_contains "check failed" $OUT_DIR/sync_diff.log
rm -f $OUT_DIR/sync_diff.log

echo "ignore check column b, check result should be pass"
sed 's/\[""\]#IGNORE/["b"]/g' config.toml > config_.toml
sync_diff_inspector --config=./config_.toml > $OUT_DIR/ignore_column_diff.output || true
check_contains "check pass!!!" $OUT_DIR/sync_diff.log
rm -f $OUT_DIR/sync_diff.log

echo "set range a < 10 OR a > 200, check result should be pass"
sed 's/"TRUE"#RANGE"a < 10 OR a > 200"/"`table` < 10 OR `table` > 200"/g' config.toml > config_.toml
sync_diff_inspector --config=./config_.toml > $OUT_DIR/ignore_column_diff.output || true
check_contains "check pass!!!" $OUT_DIR/sync_diff.log
rm -f $OUT_DIR/sync_diff.log

echo "execute fix.sql and use base config, and then compare data, data should be equal"
cat $OUT_DIR/*/fix-on-tidb/*.sql | mysql -uroot -h127.0.0.1 -P 4000
sync_diff_inspector --config=./config.toml > $OUT_DIR/ignore_column_diff.log || true
check_contains "check pass!!!" $OUT_DIR/sync_diff.log
rm -f $OUT_DIR/sync_diff.log

echo "table_config test passed"
