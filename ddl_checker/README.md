## DDLChecker

DDLChecker is a tool for checking if DDL SQL can be successfully executed by TiDB.

## How to use

```
Usage of ./ddl_checker:
  -host string
        MySQL host (default "127.0.0.1")
  -password string
        Password
  -port int
        MySQL port (default 3306)
  -schema string
        Schema
  -user string
        User name (default "root")


cd ddl_checker
go build
./ddl_checker --host [host] --port [port] --user [user] --password [password] --schema [schema]
```

## Modes

You can switch modes using the `SETMOD` command.
Auto mode: The program will automatically synchronize the dependent table structure from MySQL and delete the conflict table
Prompt mode: The program will ask you before synchronizing the dependent table structure from MYSQL
Offline mode: This program doesn't need to connect to MySQL, and doesn't perform anything other than executing the input SQL.

SETMOD usage: SETMOD <MODCODE>; MODCODE = ["Auto", "Prompt", "Offline"] (case insensitive).

## Example

```
# ./bin/ddl_checker --host 127.0.0.1 --port 3306 --user root --password 123 --schema test

[Auto] > ALTER TABLE table_name MODIFY column_1 int(11) NOT NULL;
[DDLChecker] Syncing Table table_name
[DDLChecker] SQL execution succeeded

[Auto] > SETMOD PROMPT;
[Prompt] > ALTER TABLE table_name MODIFY column_1 int(11) NOT NULL;
[DDLChecker] Do you want to synchronize table [table_name] from MySQL and drop table [] in ExecutableChecker?(Y/N)y
[DDLChecker] Syncing Table table_name
[DDLChecker] SQL execution succeeded

[Prompt] > setmod offline;
[Offline] > ALTER TABLE table_name MODIFY column_1 int(11) NOT NULL;
[DDLChecker] SQL execution failed: [schema:1146]Table 'test.table_name' doesn't exist

```

## License
Apache 2.0 license. See the [LICENSE](../LICENSE) file for details.

