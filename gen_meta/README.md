mporter

importer is a tool for generating and inserting datas to database which is compatible with MySQL protocol, like MySQL, TiDB.

## How to use

```
Usage of importer:
  -D string
      set the database name (default "test")
  -L string
      log level: debug, info, warn, error, fatal (default "info")
  -P int
      set the database host port (default 3306)
  -b int
      insert batch commit count (default 1)
  -c int
      parallel worker count (default 1)
  -config string
      Config file
  -h string
      set the database host ip (default "127.0.0.1")
  -i string
      create index sql
  -n int
      total job count (default 1)
  -p string
      set the database password
  -t string
      create table sql
  -u string
      set the database user (default "root")
```
