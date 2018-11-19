#!/bin/bash

source ./util.sh

TEST_DATABASE_NAME=import_test
CHECKER_EXEC="../bin/checker --host ${MYSQL_HOST} --port ${MYSQL_PORT} --user root ${TEST_DATABASE_NAME}"
MYSQL_EXEC="mysql -h ${MYSQL_HOST} -P ${MYSQL_PORT} -u root"

init(){
    check_db_status "${MYSQL_HOST}" "${MYSQL_PORT}" mysql
    ${MYSQL_EXEC} -e "drop database if exists ${TEST_DATABASE_NAME};"
    ${MYSQL_EXEC} -e "create database ${TEST_DATABASE_NAME};"
    ${MYSQL_EXEC} -e "create table ${TEST_DATABASE_NAME}.ta(a int primary key, b double, c varchar(10), d date unique, e time unique, f timestamp unique, g date unique, h datetime unique, i year unique);"
    ${MYSQL_EXEC} -e "create table ${TEST_DATABASE_NAME}.tb(a int comment '[[range=1,10]]');"
    ${MYSQL_EXEC} -e "create table ${TEST_DATABASE_NAME}.tc(a int unique comment '[[step=2]]');"
    ${MYSQL_EXEC} -e "create table ${TEST_DATABASE_NAME}.td(a int comment '[[set=1,2,3]]');"

}

destroy(){
    ${MYSQL_EXEC} -e "drop database if exists ${TEST_DATABASE_NAME};"
    checkExit
}

set -e
init
${CHECKER_EXEC}
checkExit
destroy

