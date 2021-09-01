
#!/bin/sh

set -e

cd "$(dirname "$0")"

OUT_DIR=/tmp/tidb_tools_test/sync_diff_inspector
rm -rf ./output

echo "update data in column b (WHERE a >= 10 AND a <= 200), data should not be equal"
mysql -uroot -h 127.0.0.1 -P 4000 -e "update diff_test.test set b = 'abc' where a >= 10 AND a <= 200"

rm $OUT_DIR/fix.sql || true
sync_diff_inspector --config=./config.toml > $OUT_DIR/ignore_column_diff.output || true
check_contains "check failed" ./output/sync_diff.log
rm -f ./output/sync_diff.log

echo "ignore check column b, check result should be pass"
sed 's/[""]#IGNORE/["b"]/g' config.toml > config_.toml
sync_diff_inspector --config=./config_.toml > $OUT_DIR/ignore_column_diff.output || true
check_contains "check pass!!!" ./output/sync_diff.log
rm -f ./output/sync_diff.log

echo "set range a < 10 OR a > 200, check result should be pass"
sed 's/"TRUE"#RANGE"a < 10 OR a > 200"/"a < 10 OR a > 200"/g' config.toml > config_.toml
sync_diff_inspector --config=./config_.toml > $OUT_DIR/ignore_column_diff.output || true
check_contains "check pass!!!" ./output/sync_diff.log
rm -f ./output/sync_diff.log

echo "execute fix.sql and use base config, and then compare data, data should be equal"
cat ./output/*/fix-on-tidb/*.sql | mysql -uroot -h127.0.0.1 -P 4000
sync_diff_inspector --config=./config_base.toml > $OUT_DIR/ignore_column_diff.log
check_contains "check pass!!!" $OUT_DIR/ignore_column_diff.log
rm -f ./output/sync_diff.log

echo "table_config test passed"
