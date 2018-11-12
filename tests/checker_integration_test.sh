#!/bin/bash

TEST_DATABASE_NAME=import_test
CHECKER_EXEC="../bin/checker --host ${MYSQL_HOST} --port ${MYSQL_PORT} --user root ${TEST_DATABASE_NAME}"
MYSQL_EXEC="mysql -h ${MYSQL_HOST} -P ${MYSQL_PORT} -u root"

init(){
    ${MYSQL_EXEC} -e "drop database if exists ${TEST_DATABASE_NAME};"
    checkExit
    ${MYSQL_EXEC} -e "create database ${TEST_DATABASE_NAME};"
    checkExit
    ${MYSQL_EXEC} -e "create table ${TEST_DATABASE_NAME}.ta(a int primary key, b double, c varchar(10), d date unique, e time unique, f timestamp unique, g date unique, h datetime unique, i year unique);"
    checkExit
    ${MYSQL_EXEC} -e "create table ${TEST_DATABASE_NAME}.tb(a int comment '[[range=1,10]]');"
    checkExit
    ${MYSQL_EXEC} -e "create table ${TEST_DATABASE_NAME}.tc(a int unique comment '[[step=2]]');"
    checkExit
    ${MYSQL_EXEC} -e "create table ${TEST_DATABASE_NAME}.td(a int comment '[[set=1,2,3]]');"
    checkExit

}

destroy(){
    ${MYSQL_EXEC} -e "drop database if exists ${TEST_DATABASE_NAME};"
    checkExit
}

checkExit(){
    if [[ $? -ne 0 ]]; then
        exit 1
    fi
}


init
${CHECKER_EXEC}
checkExit
destroy

