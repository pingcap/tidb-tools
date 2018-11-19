#!/bin/bash

check_db_status() {
    while true
    do
        if mysqladmin -h "$1" -P "$2" -u root ping > /dev/null 2>&1
        then
            break
        fi
        sleep 1
    done
    echo "$3 is alive"
}