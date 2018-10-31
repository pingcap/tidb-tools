Using Checkpoints
=================

Importing a large database usually takes hours and days, and if such long running processes
spuriously crashes, it would be very time-wasting to redo the previously completed tasks. Lightning
uses *checkpoints* to stores the import progress, so that restarting `tidb-lightning` will continue
importing from where it lefts off.

```toml
[checkpoint]
# Whether to enable checkpoints.
# While importing, Lightning will record which tables have been imported, so
# even if Lightning or other component crashed, we could start from a known
# good state instead of redoing everything.
enable = true

# The schema name (database name) to store the checkpoints
schema = "tidb_lightning_checkpoint"

# The data source name (DSN) in the form "USER:PASS@tcp(HOST:PORT)/".
# If not specified, the TiDB server from the [tidb] section will be used to
# store the checkpoints. You could also specify a different MySQL-compatible
# database server to reduce the load of the target TiDB cluster.
#dsn = "root@tcp(127.0.0.1:4000)/"

# Whether to keep the checkpoints after all data are imported. If false, the
# checkpoints will be deleted. Keeping the checkpoints can aid debugging but
# will leak metadata about the data source.
#keep-after-success = false
```

Storage
-------

Checkpoints are saved in any databases compatible with MySQL 5.7 or above, including MariaDB and
TiDB. By default the checkpoints are saved in the target database.

While using the target database as the checkpoint storage, Lightning is importing large amount
of data at the same time. This puts extra stress on the target database and sometimes leads
to communication timeout. Therefore, **we strongly recommend you install a temporary MySQL server to
store these checkpoints**. This server can be installed on the same host as `tidb-lightning` and can
be uninstalled after the importer progress is completed.

Checkpoint control
------------------

If `tidb-lightning` exits abnormally due to unrecoverable errors (e.g. data corruption), it will
refuse to reuse the checkpoints until the errors are resolved. This is to prevent worsening the
situation. The checkpoint errors can be resolved using the `tidb-lightning-ctl` program.

### `--checkpoint-error-destroy`

```sh
tidb-lightning-ctl --checkpoint-error-destroy='`schema`.`table`
```

If importing the table `` `schema`.`table` `` failed previously, this

1. DROPs the table `` `schema`.`table` `` from the target database, i.e. removing all imported data.
2. resets the checkpoint record of this table to be "not yet started".

If there is no errors involving the table `` `schema`.`table` ``, this operation does nothing.

This option allows us to restarting importing the table from scratch. The schema and table names
must be quoted by backquotes and is case-sensitive.

```sh
tidb-lightning-ctl --checkpoint-error-destroy=all
```

Same as applying the above on every table. This is the most convenient, safe and conservative
solution to fix the checkpoint error problem.

### `--checkpoint-error-ignore`

```sh
tidb-lightning-ctl --checkpoint-error-ignore='`schema`.`table`'
tidb-lightning-ctl --checkpoint-error-ignore=all
```

If importing the table `` `schema`.`table` `` failed previously, this clears the error status as
if nothing happened. The `all` variant applies this operation to all tables.

This should only be used when you are sure that the error can indeed be ignored. If not, some
imported data could be lost. The only safety net is the final "checksum" check, and thus the
"checksum" option should always be enabled when using `--checkpoint-error-ignore`.

### `--checkpoint-remove`

```sh
tidb-lightning-ctl --checkpoint-remove='`schema`.`table`'
tidb-lightning-ctl --checkpoint-remove=all
```

Simply remove all checkpoint information about one table / all tables, regardless of their status.

### `--checkpoint-dump`

```sh
tidb-lightning-ctl --checkpoint-dump=output/directory
```

Dumps the content of the checkpoint into the given directory. Mainly used for debugging by technical
staff.

