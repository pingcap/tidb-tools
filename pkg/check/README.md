# Check library

check is a library to define some checkers to check configurations of system. 

It is mainly used for configuration checking of data synchronization between database systems.


# Introduction


### MySQL slave privilege Checker

In MySQL data synchronization, `REPLICATION SLAVE` and `REPLICATION CLIENT` are required, `RELOAD` is strongly suggested to have.

### MySQL Version Checker

Syncer only support 5.5 <= version <= 8.0 now

### MySQL Server ID Checker

MySQL Server(master) should have server ID in master-slave replications.

### MySQL Binlog Enable Checker

MySQL Server(master) should enable binlog in master-slave replication.

### MySQL Binlog Format Checker

Syncer only support `ROW` binlog format

### MySQL Binlog Row Image Checker

`binlog_row_image` is introduced since mysql 5.6.2, and mariadb 10.1.6. In MySQL 5.5 and earlier, full row images are always used for both before images and after images.

ref:
- https://dev.mysql.com/doc/refman/5.6/en/replication-options-binary-log.html#sysvar_binlog_row_image
- https://mariadb.com/kb/en/library/replication-and-binary-log-server-system-variables/#binlog_row_image