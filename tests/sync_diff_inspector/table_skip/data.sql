drop database if exists diff_test;
create database diff_test;
create table diff_test.t0 (a int, b int, primary key(a));
create table diff_test.t1 (a int, b int, primary key(a));
insert into diff_test.t0 values (1,1);
insert into diff_test.t1 values (1,1);