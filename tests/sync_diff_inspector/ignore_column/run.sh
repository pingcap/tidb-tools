
#!/bin/sh

set -e

cd "$(dirname "$0")"

OUT_DIR=/tmp/tidb_tools_test/sync_diff_inspector

echo "update data in column b, data should not be equal"
mysql -uroot -h 127.0.0.1 -P 4000 -e "update diff_test.test set b = 'abc' limit 1"

sync_diff_inspector --config=./config_base.toml > $OUT_DIR/diff.log || true
check_contains "sourceDB don't equal targetDB" $OUT_DIR/diff.log

echo "ignore check column b, check result should be pass"
sync_diff_inspector --config=./config.toml > $OUT_DIR/diff.log || true
check_contains "test pass!!!" $OUT_DIR/diff.log
