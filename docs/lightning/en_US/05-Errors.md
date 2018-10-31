Common Errors
=============

When Lightning encounters an unrecoverable error, it exits with nonzero exit code and leaves the
reason in the log file. Errors are typically printed at the end of the log. You can also search for
the string `[error]` to look for non-fatal errors.

Here are some commonly encountered errors in the `tidb-lightning` log file and their resolution.

## checksum failed: checksum mismatched remote vs local

**Cause**: The checksum of a table in the local data source and the remote imported database differ.
There are several deeper reasons of this error:

1. The table might have already been populated before. These old data would affect the final checksum.

2. If the table does not have an integer PRIMARY KEY, some rows might be imported repeatedly between
    checkpoints. This is a known bug to be fixed in the next release.

3. If the remote checksum is 0 i.e. nothing is imported, it is possible that the cluster is too hot
    and failed to take it any data.

4. If the data is mechanically generated, ensure it respects the constrains of the table:

    * AUTO_INCREMENT columns should be positive, and should not contain the value "0".
    * There should be no duplicated entries in UNIQUE and PRIMARY KEYs.

**Solutions**:

1. Delete the corrupted data with `tidb-lightning-ctl --error-checkpoint-destroy=all`, and restart
    Lightning to import the affected tables again.

2. Consider using an external database to store the checkpoints (change `[checkpoint] dsn`) to
    reduce the target database's load.

## ResourceTemporarilyUnavailable("Too many open engines …: 8")

**Cause**: The number of concurrent engine files exceeds the limit imposed by `tikv-importer`. This
could be caused by misconfiguration. Additionally, if `tidb-lightning` exited abnormally, an engine
file might be left at a dangling open state, which could cause this error as well.

**Solutions**:

1. Increase the value of `max-open-engine` setting in `tikv-importer.toml`. This value is typically
    dictated by the available memory. This could be calculated as:

    > Max Memory Usage ≈ `max-open-engine` × `write-buffer-size` × `max-write-buffer-number`

2. Decrease the value of `table-concurrency` so it is less than `max-open-engine`.

3. Restart `tikv-importer` to forcefully remove all engine files. This also removes all
    partially imported tables, thus running `tidb-lightning-ctl --error-checkpoint-destroy=all` is
    required.

## cannot guess encoding for input file, please convert to UTF-8 manually

**Cause**: Lightning only recognizes the UTF-8 and GB-18030 encodings for the table schemas. This
error is emitted if the file isn't in any of these encodings. It is also possible that the file has
mixed encoding e.g. contains a string in UTF-8 and another string in GB-18030, due to historical
`ALTER TABLE` executions.

**Solutions**:

1. Fix the schema so that the file is entirely in either UTF-8 or GB-18030.

2. Manually CREATE the affected tables in the target database, then set
    `[mydumper] no-schema = true` to skip automatic table creation.

<!-- TODO: Add more. Check with ops. -->
