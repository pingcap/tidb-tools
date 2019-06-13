#!/bin/sh

set -e

cd "$(dirname "$0")"

OUT_DIR=/tmp/tidb_tools_test/sync_diff_inspector

mkdir $OUT_DIR || true

echo "delete one row, and will check failed and save checkpoint. when start diff again will only check the failed chunk"
mv config_base.toml config.toml
sed -i -- 's/use-checkpoint = false/use-checkpoint = true/g' config.toml || true

mysql -uroot -h 127.0.0.1 -P 4000 -e "delete from diff_test.test limit 1"
sync_diff_inspector --config=./config.toml > $OUT_DIR/checkpoint_diff.log || true
check_contains "sourceDB don't equal targetDB" $OUT_DIR/checkpoint_diff.log

cat $OUT_DIR/fix.sql | mysql -uroot -h127.0.0.1 -P 4000
sync_diff_inspector --config=./config.toml > $OUT_DIR/checkpoint_diff.log || true
check_contains "use checkpoint to load chunks" $OUT_DIR/checkpoint_diff.log
check_contains "test pass!!!" $OUT_DIR/checkpoint_diff.log

# because use checkpoint, so will only check the failed chunk
chunk_num=`grep "checksum is equal" -c $OUT_DIR/checkpoint_diff.log`
if [ $chunk_num -neq 1 ]; then
    echo "check more than one chunk"
    exit 2
fi