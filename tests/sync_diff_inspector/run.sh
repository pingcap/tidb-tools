#!/bin/sh

set -e

cd "$(dirname "$0")"

OUT_DIR=/tmp/tidb_tools_test/sync_diff_inspector

stop_services() {
    killall -9 tikv-server || true
    killall -9 pd-server || true
    killall -9 tidb-server || true
}

ts=""
get_ts() {
    port=${1-4000}
    mysql -uroot -h 127.0.0.1 -P $port -e "show master status" > $OUT_DIR/ts.log
    cat $OUT_DIR/ts.log
    ts=`grep -oE "[0-9]+" $OUT_DIR/ts.log`
    echo "get ts $ts"
}

start_upstream_tidb() {
    port=${1-4000}
    echo "Starting TiDB at port: $port..."
    tidb-server \
        -P $port \
        --store tikv \
        --path 127.0.0.1:2379 \
        --log-file "$OUT_DIR/tidb.log" &

    echo "Verifying TiDB is started..."
    check_db_status "127.0.0.1" $port "tidb"
}

start_services() {
    stop_services

    echo "Starting PD..."
    pd-server \
        --client-urls http://127.0.0.1:2379 \
        --log-file "$OUT_DIR/pd.log" \
        --data-dir "$OUT_DIR/pd" &
    # wait until PD is online...
    while ! curl -o /dev/null -sf http://127.0.0.1:2379/pd/api/v1/version; do
        sleep 1
    done

    # Tries to limit the max number of open files under the system limit
    cat - > "$OUT_DIR/tikv-config.toml" <<EOF
[rocksdb]
max-open-files = 4096
[raftdb]
max-open-files = 4096
[raftstore]
# true (default value) for high reliability, this can prevent data loss when power failure.
sync-log = false
EOF

    echo "Starting TiKV..."
    tikv-server \
        --pd 127.0.0.1:2379 \
        -A 127.0.0.1:20160 \
        --log-file "$OUT_DIR/tikv.log" \
        -C "$OUT_DIR/tikv-config.toml" \
        -s "$OUT_DIR/tikv" &
    sleep 2

    echo "Starting TiDB..."
    tidb-server \
        -P 4000 \
        --store tikv \
        --path 127.0.0.1:2379 \
        --log-file "$OUT_DIR/tidb.log" &

    echo "Verifying TiDB is started..."
    check_db_status "127.0.0.1" 4000 "tidb"

    echo "Starting Upstream TiDB..."
    tidb-server \
        -P 4001 \
        --path=$OUT_DIR/tidb \
        --status=20080 \
        --log-file "$OUT_DIR/down_tidb.log" &

    echo "Verifying Upstream TiDB is started..."
    check_db_status "127.0.0.1" 4001 "tidb"
}

trap stop_services EXIT
start_services

echo "use importer to generate test data"
mysql -uroot -h 127.0.0.1 -P 4000 -e "create database if not exists diff_test"
importer -t "create table diff_test.test(a int, b varchar(10), c float, d datetime);" -c 10 -n 10000 -P 4000 -h 127.0.0.1 -D diff_test -b 1000

echo "dump data and then load to tidb"
mydumper --host 127.0.0.1 --port 4000 --user root --outputdir $OUT_DIR/dump_diff -B diff_test -T test

loader -h 127.0.0.1 -P 4001 -u root -d $OUT_DIR/dump_diff

echo "use sync_diff_inspector to compare data"
cp config_template.toml config.toml

sync_diff_inspector --config=./config.toml > $OUT_DIR/diff.log

check_contains "test pass!!!" $OUT_DIR/diff.log
get_ts

echo "delete one data, and use snapshot compare data"
mysql -uroot -h 127.0.0.1 -P 4001 -e "delete from diff_test.test limit 1"

echo "snapshot = \"$ts\"" >> config.toml

sync_diff_inspector --config=./config.toml > $OUT_DIR/diff.log
check_contains "test pass!!!" $OUT_DIR/diff.log

echo "remove snapshot, diff should not passed"
cp config_template.toml config
sync_diff_inspector --config=./config.toml > $OUT_DIR/diff.log
check_contains "sourceDB don't equal targetDB" $OUT_DIR/diff.log

