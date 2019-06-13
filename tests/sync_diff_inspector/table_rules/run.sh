#!/bin/sh

set -e

cd "$(dirname "$0")"

OUT_DIR=/tmp/tidb_tools_test/sync_diff_inspector

mkdir $OUT_DIR || true

echo "rename table and compare data use table-rules"
mysql -uroot -h 127.0.0.1 -P 4001 -e "rename table diff_test.test to diff_test.test2"

sync_diff_inspector --config=./config_table_rule.toml > $OUT_DIR/table_rule_diff.log
check_contains "test pass!!!" $OUT_DIR/table_rule_diff.log

echo "set table rule in table's config and compare data"
sync_diff_inspector --config=./config_table_config.toml > $OUT_DIR/table_rule_diff.log
check_contains "test pass!!!" $OUT_DIR/table_rule_diff.log

# rename table back
mysql -uroot -h 127.0.0.1 -P 4001 -e "rename table diff_test.test2 to diff_test.test"