## DDLChecker

DDLChecker is a tool for checking if DDL SQL can be successfully executed by TiDB.

## How to use

```
Usage of checker:
  --host string
        MySQL host (default "127.0.0.1")
  --port int
        MySQL port (default 3306)
  --user string
        MySQL username (default "root")
  --password string
        MySQL password (default "")
  --schema string
        MySQL schema (default "")

cd ddl_checker
go build
./ddl_checker --host [host] --port [port] --user [user] --password [password] --schema [schrma]
```

## Modes

You can switch modes using the `SETMOD` command.  
Auto mode: The program will automatically synchronize the dependent table structure from MYSQL and delete the conflict table.   
Query mode: The program will ask you before synchronizing the dependent table structure from MYSQL.  
Manual mode: This program does not perform anything other than executing the input SQL.  
SETMOD Usage: SETMOD <MODCODE>; MODCODE = [0(Auto), 1(Query), 2(Manual)] 


## Example

```
# ./bin/checker --host 127.0.0.1 --port 3306 --user root --password 123 --schema test

[Auto] > ALTER TABLE table_name MODIFY column_1 int(11) NOT NULL;
[DDLChecker] Syncing Table table_name
[DDLChecker] SQL execution succeeded

[Auto] > SETMOD 1;
[Query] > ALTER TABLE table_name MODIFY column_1 int(11) NOT NULL;
[DDLChecker] Do you want to synchronize table [table_name] from MySQL and drop table [] in ExecutableChecker?(Y/N)y
[DDLChecker] Syncing Table table_name
[DDLChecker] Table table_name is exist,Skip
[DDLChecker] SQL execution succeeded

[Query] > setmod 2;
[Manual] > ALTER TABLE table_name MODIFY column_1 int(11) NOT NULL;
[DDLChecker] SQL execution failed: [schema:1146]Table 'test.table_name' doesn't exist

```

## License
Apache 2.0 license. See the [LICENSE](../LICENSE) file for details.

