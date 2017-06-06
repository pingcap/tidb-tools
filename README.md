## tidb-tools

tidb-tools are some useful tool collections for [TiDB](https://github.com/pingcap/tidb).


## How to build

```
make build # build all tools

make importer # build importer

make checker # build checker
```

When build successfully, you can find the binary in bin directory.

## Tool list

[importer](./importer)

A tool for generating and inserting datas to database which is compatible with MySQL protocol, like MySQL, TiDB.

[checker](./checker)

A tool for checking the compatibility of an existed MySQL database with TiDB.

[scripts](./scripts)

Collection of auxiliary scripts.

## License
Apache 2.0 license. See the [LICENSE](./LICENSE) file for details.
