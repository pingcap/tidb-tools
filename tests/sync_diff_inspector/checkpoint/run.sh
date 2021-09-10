#!/bin/sh

set -ex

cd "$(dirname "$0")"

OUT_DIR=/tmp/tidb_tools_test/sync_diff_inspector/output
rm -rf $OUT_DIR
mkdir -p $OUT_DIR


sed "s/\"127.0.0.1\"#MYSQL_HOST/\"${MYSQL_HOST}\"/g" ./config_base.toml | sed "s/3306#MYSQL_PORT/${MYSQL_PORT}/g" > ./config.toml

echo "================test bucket checkpoint================="
echo "---------1. chunk is in the last of the bucket---------"
export GO_FAILPOINTS="github.com/pingcap/tidb-tools/sync_diff_inspector/splitter/check-one-bucket=return();\
github.com/pingcap/tidb-tools/sync_diff_inspector/splitter/print-chunk-info=return();\
github.com/pingcap/tidb-tools/sync_diff_inspector/main/wait-for-checkpoint=return()"
sync_diff_inspector --config=./config.toml > $OUT_DIR/checkpoint_diff.output
check_contains "check pass!!!" $OUT_DIR/sync_diff.log
echo $(grep 'print-chunk-info' $OUT_DIR/sync_diff.log | awk 'END {print}' | sed 's/"]/"/g')
last_chunk_bound=$(grep 'print-chunk-info' $OUT_DIR/sync_diff.log | awk 'END {print}' | sed 's/"]/"/g' | awk -F '=' '{print $3}')
echo "$last_chunk_bound"
cp $OUT_DIR/sync_diff.log $OUT_DIR/sync_diff.log_
#rm -f $OUT_DIR/sync_diff.log
export GO_FAILPOINTS="github.com/pingcap/tidb-tools/sync_diff_inspector/splitter/print-chunk-info=return()"
sync_diff_inspector --config=./config.toml > $OUT_DIR/checkpoint_diff.output
grep 'print-chunk-info' $OUT_DIR/sync_diff.log | awk 'NR==1' > $OUT_DIR/first_chunk_bound
check_contains "lowerBounds=${last_chunk_bound}" $OUT_DIR/first_chunk_bound 


echo "--------2. chunk is in the middle of the bucket--------"
export GO_FAILPOINTS="github.com/pingcap/tidb-tools/sync_diff_inspector/splitter/check-one-bucket=return();\
github.com/pingcap/tidb-tools/sync_diff_inspector/splitter/ignore-last-chunk-in-one-bucket=return()"