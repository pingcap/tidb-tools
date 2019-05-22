#!/bin/sh

set -eu

OUT_DIR=/tmp/tidb_tools_test

mkdir -p $OUT_DIR || true
# to the dir of this script
cd "$(dirname "$0")"

pwd=$(pwd)

export PATH=$PATH:$pwd/_utils
export PATH=$PATH:$(dirname $pwd)/bin

rm -rf $OUT_DIR || true

# set to the case name you want to run only for debug
do_case=""

for script in ./*/run.sh; do
    test_name="$(basename "$(dirname "$script")")"
    if [[ $do_case != "" && $test_name != $do_case ]]; then
        continue
    fi

    echo "Running test $script..."
    PATH="$pwd/../bin:$pwd/_utils:$PATH" \
    OUT_DIR=$OUT_DIR \
    TEST_NAME=$test_name \
    sh "$script"
done

# with color
echo "\033[0;36m<<< Run all test success >>>\033[0m"
